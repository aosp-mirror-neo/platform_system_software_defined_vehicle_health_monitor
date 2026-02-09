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

use crate::hb_config::HeartbeatMonitoringConfig;
use crate::hb_monitor::HeartbeatEvent;
use crate::hb_monitor::HeartbeatMonitor;
use crate::hb_stream_ops::into_heartbeats;
use crate::{monitor_model::EntityMonitor, power_state::PowerStateReceiverFactory};
use futures::stream;
use futures::stream::StreamExt;
use futures::Stream;
use mockall::automock;
use sdv_comms::dt::SubscriberOptions;
use sdv_comms::dt::SubscriberStream;
use sdv_comms::id::UnitIdentifier;
use sdv_comms::sd::ServiceUnitChangeEvent;
use sdv_comms::sd::UnitNameDiscoveryArgs;
use sdv_comms::ContextRef;
use sdv_comms::{
    id::ServiceFqin,
    sd::{ServiceDiscoveryManager, ServiceUnitDefinition, UnitType},
};
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
        service_discovery: T::ServiceDiscoveryManager,
        power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory>,
    ) -> Self {
        Self { context, service_discovery: service_discovery.into(), power_state_receiver_factory }
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

        let stream = hb_sud_id_to_hb_stream::<T>(context, sud_id);

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
        .map_err(|_timeout| SdvStatus::new(sdv_status_rs::SdvStatusCode::DeadlineExceeded))
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

/// Constructs stream of hearbeats from a HB sud future *synchronously*: the initial future
/// is polled by the consumer of the constructed stream
fn hb_sud_id_to_hb_stream<T: Comms>(
    context: T::ContextRef,
    hb_sud_id: impl Future<Output = SdvResult<T::UnitIdentifier>> + Send,
) -> impl Stream<Item = SdvResult<HeartbeatEvent>> + Send {
    stream::once(hb_sud_id)
        .then(move |res| async move {
            match res {
                Ok(id) => {
                    T::create_untyped_stream(
                        context,
                        id,
                        Duration::ZERO,
                        SubscriberOptions { fetch_last_message: true },
                    )
                    .await
                }
                Err(e) => Err(e),
            }
        })
        .map(|res| match res {
            Ok(s) => s.pipe(Box::pin).pipe(|s| into_heartbeats(s)).left_stream(),
            Err(e) => stream::once(async { Err(e) }).right_stream(),
        })
        .flatten()
}

fn build_sd_query_for_all_suds_of_fqin(fqin: &ServiceFqin) -> SdvResult<UnitNameDiscoveryArgs> {
    UnitNameDiscoveryArgs::new_builder()
        .set_sdv_vm_name(fqin.get_sdv_vm_name())
        .set_sdv_package_name(fqin.get_sdv_package_name())
        .set_service_bundle_name(fqin.get_service_bundle_name())
        .build()
}

fn build_hb_unit_type() -> SdvResult<UnitType> {
    const PACKAGE: &str = "com.sdv.google.health";
    const BUNDLE: &str = "HealthMonitorServiceBundle";
    const TYPE_NAME: &str = "ServiceHeartbeat";
    UnitType::builder()
        .set_sdv_package_name(PACKAGE)
        .set_service_bundle_name(BUNDLE)
        .set_unit_type_name(TYPE_NAME)
        .build()
}
pub struct CommsImpl;
impl Comms for CommsImpl {
    type ContextRef = ContextRef;
    type UnitIdentifier = UnitIdentifier;
    type ServiceUnitDefinition = ServiceUnitDefinition;
    type ServiceDiscoveryManager = ServiceDiscoveryManager;

    async fn subscribe_sud_registration_by_name(
        sd: Arc<ServiceDiscoveryManager>,
        name_args: UnitNameDiscoveryArgs,
    ) -> SdvResult<impl Stream<Item = Self::ServiceUnitDefinition> + Send + Unpin + 'static> {
        sd.subscribe_service_unit_change_by_name(&name_args)
            .await?
            .filter_map(|event| {
                let sud = if let ServiceUnitChangeEvent::Registered(sud) = event {
                    Some(sud)
                } else {
                    None
                };
                future::ready(sud)
            })
            .pipe(Ok)
    }

    async fn create_untyped_stream(
        context: ContextRef,
        id: Self::UnitIdentifier,
        duration: Duration,
        options: SubscriberOptions,
    ) -> SdvResult<impl futures::Stream<Item = SdvResult<(u32, std::vec::Vec<u8>)>> + Send + 'static>
    {
        SubscriberStream::create(context, &id, duration, options).await
    }

    fn get_owning_fqin(id: &Self::UnitIdentifier) -> &ServiceFqin {
        id.get_fqin()
    }

    fn get_unit_identifier(sud: &Self::ServiceUnitDefinition) -> Self::UnitIdentifier {
        sud.get_unit_identifier()
    }

    fn get_unit_type(sud: &Self::ServiceUnitDefinition) -> UnitType {
        sud.get_unit_type()
    }
}

pub trait Comms: 'static {
    type ContextRef: Send + Sync + Copy;
    type UnitIdentifier: Send + Sync;
    type ServiceUnitDefinition: Send + Sync;
    type ServiceDiscoveryManager: Send + Sync;

    #[allow(unreachable_code)]
    fn subscribe_sud_registration_by_name(
        _sd: Arc<Self::ServiceDiscoveryManager>,
        _name_args: UnitNameDiscoveryArgs,
    ) -> impl Future<
        Output = SdvResult<
            impl Stream<Item = Self::ServiceUnitDefinition> + Send + Unpin + 'static,
        >,
    > + Send {
        unimplemented!("subscribe_sud_registration_bu_name unimplemented!");
        stream::empty().pipe(Ok).pipe(future::ready)
    }

    #[allow(unreachable_code)]
    fn create_untyped_stream(
        _context: Self::ContextRef,
        _id: Self::UnitIdentifier,
        _duration: Duration,
        _options: SubscriberOptions,
    ) -> impl Future<
        Output = SdvResult<impl Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send + 'static>,
    > + Send {
        async {
            unimplemented!("create_untyped_stream unimplemented!");
            stream::empty().pipe(Ok)
        }
    }

    fn get_owning_fqin(_id: &Self::UnitIdentifier) -> &ServiceFqin {
        unimplemented!("get_owning_fqin unimplemented!")
    }
    fn get_unit_identifier(_sud: &Self::ServiceUnitDefinition) -> Self::UnitIdentifier {
        unimplemented!("get_unit_identifier unimplemented!")
    }
    fn get_unit_type(_sud: &Self::ServiceUnitDefinition) -> UnitType {
        unimplemented!("get_unit_type unimplemented!")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hb_monitor::HeartbeatTrackEvent;
    use futures::stream;
    use health_api::health_topic::ServiceHeartbeat;
    use protobuf::well_known_types::timestamp::Timestamp;
    use protobuf::Message;
    use protobuf::MessageField;
    use std::str::FromStr;
    use tokio::time;

    #[tokio::test]
    async fn test_hb_stream_valid_when_sud_return_succeeds_and_dt_stream_creation_succeeds() {
        //SETUP
        const HB_TIMESTAMP_S: i64 = 33i64;
        const HB_TIMESTAMP_NS: i32 = 44i32;

        struct TestableComms;
        impl Comms for TestableComms {
            type ContextRef = ();
            type UnitIdentifier = ();
            type ServiceUnitDefinition = ();
            type ServiceDiscoveryManager = ();

            /// creates stream of one Heartbeat (mocking case where dt stream is created successfully)
            async fn create_untyped_stream(
                _context: Self::ContextRef,
                _id: Self::UnitIdentifier,
                _duration: Duration,
                _options: SubscriberOptions,
            ) -> SdvResult<impl Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send + 'static>
            {
                let hb = ServiceHeartbeat {
                    timestamp: MessageField::some(Timestamp {
                        seconds: HB_TIMESTAMP_S,
                        nanos: HB_TIMESTAMP_NS,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let wire = hb.write_length_delimited_to_bytes().unwrap();

                let nr_of_hearbeats = 1u32;
                (nr_of_hearbeats, wire).pipe(Ok).pipe(future::ready).pipe(stream::once).pipe(Ok)
            }
        }
        let sud_future_returning_valid_sud = future::ready(Ok(()));

        // ACT
        let stream = hb_sud_id_to_hb_stream::<TestableComms>((), sud_future_returning_valid_sud);
        tokio::pin!(stream);
        let res1 = stream
            .next()
            .pipe(|f| time::timeout(Duration::from_millis(300), f))
            .await
            .unwrap()
            .unwrap();
        let res2 = stream
            .next()
            .pipe(|f| time::timeout(Duration::from_millis(300), f))
            .await
            .unwrap()
            .unwrap();

        // ASSERT: stream returns an initial Started monitoring event
        assert!(matches!(
            res1,
            Ok(HeartbeatEvent::TrackingHeartbeat(HeartbeatTrackEvent::StartedMonitoring(_)))
        ));

        // ASSERT: stream returns a second event, with the heartbeat received from HB publication
        let res2 = if let Ok(HeartbeatEvent::TrackingHeartbeat(
            HeartbeatTrackEvent::ReceivedHearbeat(res2),
        )) = res2
        {
            res2
        } else {
            panic!("stream did not return what we expected");
        };

        let expected =
            Timestamp { seconds: HB_TIMESTAMP_S, nanos: HB_TIMESTAMP_NS, ..Default::default() };
        let expected: SystemTime = Into::into(expected);
        assert!(res2 == expected);
    }

    #[tokio::test]
    async fn test_hb_stream_produces_errors_when_sud_return_succeeds_but_dt_stream_creation_fails()
    {
        // SETUP
        struct CommsDtStreamCreationError;
        impl Comms for CommsDtStreamCreationError {
            type ContextRef = ();
            type UnitIdentifier = ();
            type ServiceUnitDefinition = ();
            type ServiceDiscoveryManager = ();

            async fn create_untyped_stream(
                _context: Self::ContextRef,
                _id: Self::UnitIdentifier,
                _duration: Duration,
                _options: SubscriberOptions,
            ) -> SdvResult<impl Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send + 'static>
            {
                Err::<stream::Pending<_>, _>(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound))
            }
        }
        let sud_future_returning_valid_sud = future::ready(Ok(()));

        // ACT
        let stream = hb_sud_id_to_hb_stream::<CommsDtStreamCreationError>(
            (),
            sud_future_returning_valid_sud,
        );
        tokio::pin!(stream);
        let res = stream.next().await;

        // ASSERT: stream produces an error, although sud was retrieved, as create raw dt stream failed
        assert!(res.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_hb_stream_produces_error_when_sud_return_fails() {
        // SETUP
        struct CommsDtStreamCreationError;
        impl Comms for CommsDtStreamCreationError {
            type ContextRef = ();
            type UnitIdentifier = ();
            type ServiceUnitDefinition = ();
            type ServiceDiscoveryManager = ();

            async fn create_untyped_stream(
                _context: Self::ContextRef,
                _id: Self::UnitIdentifier,
                _duration: Duration,
                _options: SubscriberOptions,
            ) -> SdvResult<impl Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send + 'static>
            {
                Err::<stream::Pending<_>, _>(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound))
            }
        }
        let sud_future_returning_error = future::ready(Err(SdvStatus::with_message(
            sdv_status_rs::SdvStatusCode::DeadlineExceeded,
            "Error due to timeout waiting for SUD to be registered, for example",
        )));

        // ACT
        let stream =
            hb_sud_id_to_hb_stream::<CommsDtStreamCreationError>((), sud_future_returning_error);
        tokio::pin!(stream);
        let res = stream.next().await;

        // ASSERT: stream produces an error: sud retrieval failed
        assert!(res.unwrap().is_err());
    }
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
            .set_sdv_package_name("com.sdv.google.health")
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
