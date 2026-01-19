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
use crate::hb_monitor::HeartbeatMonitor;
use crate::{monitor_model::EntityMonitor, power_state::PowerStateReceiverFactory};
use async_trait::async_trait;
use futures::stream::StreamExt;
use futures::Stream;
use health_api::health_topic::ServiceHeartbeat;
use mockall::automock;
use sdv_comms::dt::SubscriberOptions;
use sdv_comms::dt::SubscriberStream;
use sdv_comms::sd::UnitNameDiscoveryArgs;
use sdv_comms::ContextRef;
use sdv_comms::{
    id::ServiceFqin,
    sd::{ServiceDiscoveryManager, ServiceUnitDefinition, UnitType},
};
use sdv_mw_rs_comms::observe;
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use std::future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tap::prelude::*;
use tracing::instrument;

#[automock]
#[async_trait]
pub trait HeartbeatMonitorFactory: Send + Sync {
    async fn new_monitor(
        &self,
        sb_fqin: &ServiceFqin,
        config: HeartbeatMonitoringConfig,
    ) -> SdvResult<Box<dyn EntityMonitor>>;
}

pub struct HeartbeatMonitorFactoryImpl {
    context: ContextRef,
    service_discovery: Arc<dyn ServiceUnitDefinitionDiscoverer>,
    power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory>,
}

impl HeartbeatMonitorFactoryImpl {
    pub fn new(
        context: ContextRef,
        service_discovery: Arc<dyn ServiceUnitDefinitionDiscoverer>,
        power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory>,
    ) -> Self {
        Self { context, service_discovery, power_state_receiver_factory }
    }
}

#[async_trait]
impl HeartbeatMonitorFactory for HeartbeatMonitorFactoryImpl {
    #[instrument(skip(self))]
    async fn new_monitor(
        &self,
        fqin: &ServiceFqin,
        config: HeartbeatMonitoringConfig,
    ) -> SdvResult<Box<dyn EntityMonitor>> {
        let sud = get_existing_hb_sud(self.service_discovery.as_ref(), fqin).await?;
        let stream = hb_sud_to_hb_stream(self.context, &sud).await?;

        Ok(Box::new(HeartbeatMonitor::new(
            stream,
            config,
            Box::new(SystemTime::now),
            self.power_state_receiver_factory.create_receiver(),
        )))
    }
}

/// Queries SD agent for heartbeat service unit definition of fqin.
/// Assumes that publication was already registered with SD.
/// Assumption implication: although future contains a suspend point, this should be an
/// almost instantaneously completing future
async fn get_existing_hb_sud(
    service_discovery: &(impl ServiceUnitDefinitionDiscoverer + ?Sized),
    fqin: &ServiceFqin,
) -> SdvResult<ServiceUnitDefinition> {
    let query = UnitNameDiscoveryArgs::new_builder()
        .set_sdv_vm_name(fqin.get_sdv_vm_name())
        .set_sdv_package_name(fqin.get_sdv_package_name())
        .set_service_bundle_name(fqin.get_service_bundle_name())
        .build()?;

    let hb_unit_type = build_hb_unit_type()?;
    service_discovery
        .list_units_by_name(&query)
        .filter(|sud| {
            future::ready(
                sud.get_service_bundle_identity().get_fqin().get_service_instance_name()
                    == fqin.get_service_instance_name(),
            )
        })
        .filter(|sud| future::ready(sud.get_unit_type() == hb_unit_type))
        .next()
        .await
        .ok_or(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound))
}

async fn hb_sud_to_hb_stream(
    context: ContextRef,
    hb_sud: &ServiceUnitDefinition,
) -> SdvResult<Pin<Box<dyn Stream<Item = SdvResult<Vec<ServiceHeartbeat>>> + Send>>> {
    let untyped_stream = SubscriberStream::create(
        context,
        &hb_sud.get_unit_identifier(),
        Duration::ZERO, // min time between notifications
        SubscriberOptions { fetch_last_message: true },
    )
    .await?;

    untyped_stream.pipe(Box::pin).pipe(|s| observe::<ServiceHeartbeat>(s)).pipe(Ok)
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

/// Interface to ServiceUnitDefinition functionality, allowing unit testing
pub trait SudProvider: Send {
    fn get_unit_name(&self) -> String;
    fn get_owning_fqin(&self) -> ServiceFqin;
}

impl SudProvider for ServiceUnitDefinition {
    fn get_unit_name(&self) -> String {
        self.get_unit_identifier().get_unit_name().to_owned()
    }
    fn get_owning_fqin(&self) -> ServiceFqin {
        self.get_service_bundle_identity().get_fqin().clone()
    }
}

/// Interface to ServiceDiscoveryManager functionality, allowing unit testing
pub trait ServiceUnitDefinitionDiscoverer: Send + Sync {
    fn list_units_by_name(
        &self,
        name_args: &UnitNameDiscoveryArgs,
    ) -> Pin<Box<dyn Stream<Item = ServiceUnitDefinition> + Send>>;
}
impl ServiceUnitDefinitionDiscoverer for ServiceDiscoveryManager {
    fn list_units_by_name(
        &self,
        name_args: &UnitNameDiscoveryArgs,
    ) -> Pin<Box<dyn Stream<Item = ServiceUnitDefinition> + Send>> {
        Box::pin(self.list_units_by_name(name_args))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::power_state::MockPowerStateReceiverFactory;
    use crate::power_state::PowerStateInfo;
    use futures::stream;
    use mockall::mock;
    use mockall::predicate::eq;
    use sdv_mw_rs_comms::ByteBuffersStream;
    use sdv_mw_rs_comms::InstantRead;
    use sdv_mw_rs_comms::MockCommunicate;
    use sdv_mw_rs_comms::Publication;
    use sdv_mw_rs_comms::ReadHistory;
    use sdv_mw_rs_comms::Subscribe;
    use sdv_mw_rs_comms::SubscribeOptions;
    use sdv_mw_rs_comms::UnitName;
    use sdv_mw_rs_comms::UnitQuery;
    use std::str::FromStr;
    use tokio::sync::watch;

    const MONITORED_FQIN_VM: &str = "vm";
    const MONITORED_FQIN_PACKAGE: &str = "com.sdv.pkg";
    const MONITORED_FQIN_BUNDLE: &str = "Bundle";
    const MONITORED_FQIN_INSTANCE: &str = "instance";
    const CUSTOM_UNIT_NAME: &str = "my_unit_name";

    mock! {
        pub Publication {} // mock type name: MockPublication
        #[async_trait::async_trait]
        impl Publication for Publication{
            async fn subscribe(&self, options: &SubscribeOptions) -> SdvResult<Box<dyn Subscribe>>;
            async fn observe(&self, options: &SubscribeOptions) -> SdvResult<ByteBuffersStream>;
            async fn create_history(&self, options: &SubscribeOptions) -> SdvResult<Box<dyn ReadHistory>>;
            fn create_instant_read(&self) -> Box<dyn InstantRead>;
        }
    }

    struct ServiceDiscoveryMock;
    impl ServiceUnitDefinitionDiscoverer for ServiceDiscoveryMock {
        fn list_units_by_name(
            &self,
            _name_args: &UnitNameDiscoveryArgs,
        ) -> Pin<Box<dyn Stream<Item = ServiceUnitDefinition> + Send>> {
            unimplemented!("hb factory generics under rework");
        }
    }

    #[tokio::test]
    async fn test_new_monitor_success() {
        let fqin = ServiceFqin::from_str("vm:com.sdv.pkg.Bundle/instance").unwrap();
        let config = HeartbeatMonitoringConfig {
            initial_delay_ms: 0,
            period_ms: 0,
            num_periods: 0,
            task_duration_ms: 0,
        };

        let mut mock_comms = MockCommunicate::new();
        let mut mock_publication = MockPublication::new();

        mock_publication
            .expect_observe()
            .with(eq(SubscribeOptions::builder().read_on_subscribe(true).build().unwrap()))
            .times(1)
            .return_once(|_| Ok(Box::pin(stream::pending::<SdvResult<(u32, Vec<u8>)>>())));

        mock_comms
            .expect_find_publication()
            .withf(move |query: &UnitQuery| {
                let expected_name = UnitName::builder()
                    .vm_name(MONITORED_FQIN_VM)
                    .package_name(MONITORED_FQIN_PACKAGE)
                    .bundle_name(MONITORED_FQIN_BUNDLE)
                    .instance_name(MONITORED_FQIN_INSTANCE)
                    .service_unit_name(CUSTOM_UNIT_NAME)
                    .build()
                    .unwrap();
                query == &UnitQuery::Name(expected_name)
            })
            .times(1)
            .return_once(|_| {
                Box::pin(async { Ok(Arc::new(mock_publication) as Arc<dyn Publication>) })
            });

        let mut mock_power_state_factory = MockPowerStateReceiverFactory::new();
        mock_power_state_factory.expect_create_receiver().times(1).return_once(|| {
            let (_tx, rx) = watch::channel(PowerStateInfo::default());
            rx
        });
        let factory = HeartbeatMonitorFactoryImpl::new(
            ContextRef::create(fqin.clone()),
            Arc::new(ServiceDiscoveryMock {}),
            Arc::new(mock_power_state_factory),
        );

        let r = factory.new_monitor(&fqin, config).await;
        assert!(r.is_ok());
    }
}
