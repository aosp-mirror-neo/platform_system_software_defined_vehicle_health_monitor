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
use sdv_comms::{
    id::ServiceFqin,
    sd::{ServiceDiscoveryManager, ServiceUnitDefinition, UnitType},
};
use sdv_mw_rs_comms::observe;
use sdv_mw_rs_comms::UnitName;
use sdv_mw_rs_comms::UnitQuery;
use sdv_mw_rs_comms::{Communicate, SubscribeOptions};
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{info_span, instrument, Instrument};

const HEARTBEAT_UNIT_TYPE_PACKAGE: &str = "com.sdv.google.health";
const HEARTBEAT_UNIT_TYPE_BUNDLE: &str = "HealthMonitorServiceBundle";
const HEARTBEAT_UNIT_TYPE_NAME: &str = "ServiceHeartbeat";

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
    comms: Arc<dyn Communicate>,
    service_discovery: Arc<dyn ServiceUnitDefinitionDiscoverer>,
    power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory>,
}

impl HeartbeatMonitorFactoryImpl {
    pub fn new(
        comms: Arc<dyn Communicate>,
        service_discovery: Arc<dyn ServiceUnitDefinitionDiscoverer>,
        power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory>,
    ) -> Self {
        Self { comms, service_discovery, power_state_receiver_factory }
    }

    #[instrument(skip(self))]
    async fn find_heartbeat_publisher_service_unit_name(
        &self,
        service_fqin: &ServiceFqin,
    ) -> SdvResult<String> {
        let unit_type = UnitType::builder()
            .set_sdv_package_name(HEARTBEAT_UNIT_TYPE_PACKAGE)
            .set_service_bundle_name(HEARTBEAT_UNIT_TYPE_BUNDLE)
            .set_unit_type_name(HEARTBEAT_UNIT_TYPE_NAME)
            .build()?;

        let list_sud_span =
            info_span!("health_monitor::heartbeat_monitor_factory::list_sud").entered();
        let mut service_units_stream = self.service_discovery.list_by_type(&unit_type);
        list_sud_span.exit();

        while let Some(service_unit) = service_units_stream
            .next()
            .instrument(info_span!("health_monitor::heartbeat_monitor_factory::list_single_sud"))
            .await
        {
            if service_unit.get_owning_fqin() == *service_fqin {
                return Ok(service_unit.get_unit_name());
            }
        }

        Err(SdvStatus::with_message(
            sdv_status_rs::SdvStatusCode::NotFound,
            format!("Could not find any service units for {service_fqin}"),
        ))
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
        let service_unit_name = self.find_heartbeat_publisher_service_unit_name(fqin).await?;

        let unit_name_query = &UnitQuery::Name(
            UnitName::builder()
                .vm_name(fqin.get_sdv_vm_name())
                .package_name(fqin.get_sdv_package_name())
                .bundle_name(fqin.get_service_bundle_name())
                .instance_name(fqin.get_service_instance_name())
                .service_unit_name(service_unit_name)
                .build()?,
        );

        let publication_future = self.comms.find_publication(unit_name_query);
        let publication = publication_future
            .instrument(info_span!("health_monitor::heartbeat_monitor_factory::find_publication"))
            .await?;

        let subscriber_options = &SubscribeOptions::builder().read_on_subscribe(true).build()?;
        let untyped_stream = publication.observe(subscriber_options).await?;
        let typed_stream = observe::<ServiceHeartbeat>(Box::pin(untyped_stream));
        Ok(Box::new(HeartbeatMonitor::new(
            typed_stream,
            config,
            Box::new(SystemTime::now),
            self.power_state_receiver_factory.create_receiver(),
        )))
    }
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
    fn list_by_type(
        &self,
        unit_type: &UnitType,
    ) -> Pin<Box<dyn Stream<Item = Box<dyn SudProvider>> + Send>>;
}
impl ServiceUnitDefinitionDiscoverer for ServiceDiscoveryManager {
    fn list_by_type(
        &self,
        unit_type: &UnitType,
    ) -> Pin<Box<dyn Stream<Item = Box<dyn SudProvider>> + Send>> {
        Box::pin(
            self.list_units_by_type(unit_type).map(|sud| Box::new(sud) as Box<dyn SudProvider>),
        )
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

    struct SUDMock {}
    impl SudProvider for SUDMock {
        fn get_unit_name(&self) -> String {
            CUSTOM_UNIT_NAME.to_owned()
        }

        fn get_owning_fqin(&self) -> ServiceFqin {
            ServiceFqin::from_str(&format!(
                "{MONITORED_FQIN_VM}:{MONITORED_FQIN_PACKAGE}.{MONITORED_FQIN_BUNDLE}/{MONITORED_FQIN_INSTANCE}"
            ))
            .unwrap()
        }
    }

    struct ServiceDiscoveryMock;
    impl ServiceUnitDefinitionDiscoverer for ServiceDiscoveryMock {
        fn list_by_type(
            &self,
            _: &UnitType,
        ) -> Pin<Box<dyn Stream<Item = Box<dyn SudProvider>> + Send>> {
            Box::pin(stream::iter([Box::new(SUDMock {}) as Box<dyn SudProvider>]))
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
            Arc::new(mock_comms),
            Arc::new(ServiceDiscoveryMock {}),
            Arc::new(mock_power_state_factory),
        );

        let r = factory.new_monitor(&fqin, config).await;
        assert!(r.is_ok());
    }
}
