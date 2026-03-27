// Copyright (C) 2026 The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::commstack_trait::Comms;
use crate::hb_config::HeartbeatMonitoringConfig;
use crate::hb_monitor::HeartbeatEvent;
use crate::hb_monitor::HeartbeatMonitor;
use crate::hb_monitor::HeartbeatTrackEvent;
use crate::hb_stream_ops::hb_sud_id_to_hb_stream;
use crate::hb_stream_ops::with_monitoring_start_event;
use crate::{monitor_model::EntityMonitor, power_state::PowerStateReceiverFactory};
use futures::stream::StreamExt;
use futures::Stream;
use log::error;
use mockall::automock;
use sdv_comms::sd::UnitNameDiscoveryArgs;
use sdv_comms::{id::ServiceFqin, sd::UnitType};
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use std::future::{self, Future};
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tap::prelude::*;
use tokio::time::timeout;

#[automock]
pub trait HeartbeatMonitorFactory: Send + Sync + 'static {
    /// creates monitor. Does not assume the HB sud is already registered, rather waits
    /// asunchronously for a duration specified by configuration.
    /// Creation is done synchronously. Any waits for sd apis are delegated to the HB monitor
    fn new_monitor(
        &self,
        fqin: &ServiceFqin,
        config: HeartbeatMonitoringConfig,
    ) -> SdvResult<Box<dyn EntityMonitor>>;
}
pub struct HeartbeatMonitorFactoryImpl<T: Comms> {
    context: T::ContextRef,
    service_discovery: Arc<T::ServiceDiscoveryManager>,
    power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory>,
}

impl<T: Comms> HeartbeatMonitorFactoryImpl<T> {
    pub fn new(
        context: T::ContextRef,
        service_discovery: Arc<T::ServiceDiscoveryManager>,
        power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory>,
    ) -> Self {
        Self { context, service_discovery, power_state_receiver_factory }
    }
}

impl<T: Comms> HeartbeatMonitorFactory for HeartbeatMonitorFactoryImpl<T> {
    fn new_monitor(
        &self,
        fqin: &ServiceFqin,
        config: HeartbeatMonitoringConfig,
    ) -> SdvResult<Box<dyn EntityMonitor>> {
        let context = self.context;
        let query = build_sd_query_for_all_suds_of_fqin(fqin)?;
        let sd = Arc::clone(&self.service_discovery);

        let sud_stream = T::subscribe_sud_registration_by_name(sd, query);

        let sud_id = get_hb_sud_id_from_stream::<T>(sud_stream, fqin)?.pipe(with_sanity_timeout);

        let stream = hb_sud_id_to_hb_stream::<HeartbeatEvent, T>(context, sud_id).pipe(|s| {
            with_monitoring_start_event(
                HeartbeatEvent::TrackingHeartbeat(HeartbeatTrackEvent::StartedMonitoring(
                    SystemTime::now(),
                )),
                s,
            )
        });

        Ok(Box::new(HeartbeatMonitor::new(
            stream,
            config,
            Box::new(SystemTime::now),
            self.power_state_receiver_factory.create_receiver(),
        )) as Box<dyn EntityMonitor>)
    }
}

/// A timeout that is never expected to be hit under normal production circumstances.
///
/// ** explicit registration: SUD should be registered before RPC registration call. SUD propagation
/// from SD to HM should be much faster than 2 seconds
///
/// ** implicit registration: bundle should create HB pub asynchronously, task launched as early
/// as possible from on_start (TODO(tmot) mw code does this once vsidl integration).
/// SUD propagation should be much faster than 2 seconds.
///
/// Yet, added for easier diagnosis of problems and developer experience: if HB monitor reports
/// deadline exceeded, an issue exists with the heartbeat SUD registration, rather than with the
/// heartbeat publication.
///
/// Internal, as if bundle developer follow guidelines, this timeout should never be reached.
const SANITY_TIMEOUT: Duration = Duration::from_secs(2);
async fn with_sanity_timeout<T>(f: impl Future<Output = SdvResult<T>>) -> SdvResult<T> {
    f.pipe(|f| timeout(SANITY_TIMEOUT, f))
        .await
        .map_err(|_e| {
            error!("Sanity timeout {SANITY_TIMEOUT:?} exceeded. When using implicit hb registration, register HB pub asynchronously, from on_start()");
            SdvStatus::new(sdv_status_rs::SdvStatusCode::DeadlineExceeded)
        })
        .flatten()
}

/// function assumption: sud_stream input only contains SUDs owned by the bundle of interest
fn get_hb_sud_id_from_stream<T: Comms>(
    sud_stream: impl Future<Output = SdvResult<impl Stream<Item = T::ServiceUnitDefinition> + Send + Unpin>>
        + Send
        + 'static,
    fqin: &ServiceFqin,
) -> SdvResult<impl Future<Output = SdvResult<T::UnitIdentifier>> + Send> {
    let hb_unit_type = build_hb_unit_type()?;
    let owned_instance_name = fqin.get_service_instance_name().to_string();
    async move {
        sud_stream
            .await?
            .filter(|sud| future::ready(T::get_unit_type(sud) == hb_unit_type))
            .map(|sud| T::get_unit_identifier(&sud))
            .filter(|sud_id| {
                future::ready(
                    T::get_owning_fqin(sud_id).get_service_instance_name() == owned_instance_name,
                )
            })
            .next()
            .await
            .ok_or(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound))
    }
    .pipe(Ok)
}

fn build_sd_query_for_all_suds_of_fqin(fqin: &ServiceFqin) -> SdvResult<UnitNameDiscoveryArgs> {
    UnitNameDiscoveryArgs::new_builder()
        .set_sdv_vm_name(fqin.get_sdv_vm_name())
        .set_sdv_package_name(fqin.get_sdv_package_name())
        .set_service_bundle_name(fqin.get_service_bundle_name())
        .build()
}

fn build_hb_unit_type() -> SdvResult<UnitType> {
    const PACKAGE: &str = "com.android.sdv.health";
    const BUNDLE: &str = "HealthMonitorServiceBundle";
    const TYPE_NAME: &str = "ServiceHeartbeat";
    UnitType::builder()
        .set_sdv_package_name(PACKAGE)
        .set_service_bundle_name(BUNDLE)
        .set_unit_type_name(TYPE_NAME)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::str::FromStr;

    struct SUD {
        fqin: ServiceFqin,
        unit_type: UnitType,
    }

    struct SimpleCommsSUDOnly;
    impl Comms for SimpleCommsSUDOnly {
        type ContextRef = ();
        type UnitIdentifier = ((), ServiceFqin);
        type ServiceUnitDefinition = SUD;
        type ServiceDiscoveryManager = ();

        fn get_owning_fqin(id: &Self::UnitIdentifier) -> &ServiceFqin {
            &id.1
        }
        fn get_unit_identifier(sud: &Self::ServiceUnitDefinition) -> Self::UnitIdentifier {
            ((), sud.fqin.clone())
        }
        fn get_unit_type(sud: &Self::ServiceUnitDefinition) -> UnitType {
            sud.unit_type.clone()
        }
    }

    #[tokio::test]
    async fn sud_retrieved_when_sud_stream_contains_sud_with_right_instance_and_right_type() {
        let fqin_str: &str = "vm:com.sdv.pkg.Bundle/instance";
        let fqin = ServiceFqin::from_str(fqin_str).unwrap();

        let sud = SUD { fqin: fqin.clone(), unit_type: build_hb_unit_type().unwrap() };
        let sud_stream = sud.pipe(future::ready).pipe(stream::once).pipe(Ok).pipe(future::ready);
        let sud_id = get_hb_sud_id_from_stream::<SimpleCommsSUDOnly>(sud_stream, &fqin);

        assert!(sud_id.is_ok());
        let sud_id = sud_id.unwrap().await;
        assert!(sud_id.is_ok());
    }

    #[tokio::test]
    async fn sud_not_found_when_sud_stream_contains_sud_from_wrong_instance() {
        let fqin: &str = "vm:com.sdv.pkg.Bundle/instance";
        let fqin = ServiceFqin::from_str(fqin).unwrap();

        let other_fqin: &str = "vm:com.sdv.pkg.Bundle/otherinstance";
        let other_fqin = ServiceFqin::from_str(other_fqin).unwrap();

        let sud = SUD { fqin: other_fqin, unit_type: build_hb_unit_type().unwrap() };
        let sud_stream = sud.pipe(future::ready).pipe(stream::once).pipe(Ok).pipe(future::ready);
        let sud_id = get_hb_sud_id_from_stream::<SimpleCommsSUDOnly>(sud_stream, &fqin);

        assert!(sud_id.is_ok());
        let sud_id = sud_id.unwrap().await;
        assert!(sud_id.is_err());
    }

    #[tokio::test]
    async fn sud_not_found_when_sud_stream_contains_sud_with_wrong_type() {
        let fqin: &str = "vm:com.sdv.pkg.Bundle/instance";
        let fqin = ServiceFqin::from_str(fqin).unwrap();

        let wrong_type = UnitType::builder()
            .set_sdv_package_name("com.android.sdv.health")
            .set_service_bundle_name("HealthMonitorServiceBundle")
            .set_unit_type_name("ServiceHeartbreak")
            .build()
            .unwrap();

        let sud = SUD { fqin: fqin.clone(), unit_type: wrong_type };
        let sud_stream = sud.pipe(future::ready).pipe(stream::once).pipe(Ok).pipe(future::ready);
        let sud_id = get_hb_sud_id_from_stream::<SimpleCommsSUDOnly>(sud_stream, &fqin);

        assert!(sud_id.is_ok());
        let sud_id = sud_id.unwrap().await;
        assert!(sud_id.is_err());
    }

    #[tokio::test]
    async fn sud_not_found_when_sud_stream_is_empty() {
        let fqin_str: &str = "vm:com.sdv.pkg.Bundle/instance";
        let fqin = ServiceFqin::from_str(fqin_str).unwrap();

        let sud_stream = stream::empty().pipe(Ok).pipe(future::ready);
        let sud_id = get_hb_sud_id_from_stream::<SimpleCommsSUDOnly>(sud_stream, &fqin);

        assert!(sud_id.is_ok());
        let sud_id = sud_id.unwrap().await;
        assert!(sud_id.is_err());
    }
}
