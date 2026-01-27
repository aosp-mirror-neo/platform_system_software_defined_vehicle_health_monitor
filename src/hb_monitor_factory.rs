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
use futures::stream::{self, StreamExt};
use futures::Stream;
use health_api::health_topic::ServiceHeartbeat;
use mockall::automock;
use sdv_comms::dt::SubscriberOptions;
use sdv_comms::dt::SubscriberStream;
use sdv_comms::id::UnitIdentifier;
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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tap::prelude::*;

#[automock]
#[async_trait]
pub trait HeartbeatMonitorFactory: Send + Sync + 'static {
    async fn new_monitor(
        &self,
        sb_fqin: &ServiceFqin,
        config: HeartbeatMonitoringConfig,
    ) -> SdvResult<Box<dyn EntityMonitor>>;
}

pub struct HeartbeatMonitorFactoryImpl<T: Comms> {
    context: T::ContextRef,
    service_discovery: T::ServiceDiscoveryManager,
    power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory>,
}

impl<T: Comms> HeartbeatMonitorFactoryImpl<T> {
    pub fn new(
        context: T::ContextRef,
        service_discovery: T::ServiceDiscoveryManager,
        power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory>,
    ) -> Self {
        Self { context, service_discovery, power_state_receiver_factory }
    }
}

#[async_trait]
impl<T: Comms> HeartbeatMonitorFactory for HeartbeatMonitorFactoryImpl<T> {
    async fn new_monitor(
        &self,
        fqin: &ServiceFqin,
        config: HeartbeatMonitoringConfig,
    ) -> SdvResult<Box<dyn EntityMonitor>> {
        let sud_id = get_existing_hb_sud_id::<T>(&self.service_discovery, fqin).await?;
        let stream = hb_sud_id_to_stream::<T>(self.context, &sud_id).await?;

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
async fn get_existing_hb_sud_id<T: Comms>(
    service_discovery: &T::ServiceDiscoveryManager,
    fqin: &ServiceFqin,
) -> SdvResult<T::UnitIdentifier> {
    let query = UnitNameDiscoveryArgs::new_builder()
        .set_sdv_vm_name(fqin.get_sdv_vm_name())
        .set_sdv_package_name(fqin.get_sdv_package_name())
        .set_service_bundle_name(fqin.get_service_bundle_name())
        .build()?;

    let hb_unit_type = build_hb_unit_type()?;
    let res = {
        T::list_units_by_name(service_discovery, &query)
            .pipe(Box::pin)
            .filter(|sud| future::ready(T::get_unit_type(sud) == hb_unit_type))
            .map(|sud| T::get_unit_identifier(&sud))
            .filter(|sud_id| {
                future::ready(
                    T::get_owning_fqin(sud_id).get_service_instance_name()
                        == fqin.get_service_instance_name(),
                )
            })
            .next()
            .await
            .ok_or(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound))
    };
    res
}

async fn hb_sud_id_to_stream<T: Comms>(
    context: T::ContextRef,
    hb_sud_id: &T::UnitIdentifier,
) -> SdvResult<Pin<Box<dyn Stream<Item = SdvResult<Vec<ServiceHeartbeat>>> + Send>>> {
    let untyped_stream = T::create_untyped_stream(
        context,
        hb_sud_id,
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

pub trait Comms: 'static {
    type ContextRef: Send + Sync + Copy;
    type UnitIdentifier: Send + Sync;
    type ServiceUnitDefinition: Send + Sync;
    type ServiceDiscoveryManager: Send + Sync;

    #[allow(unreachable_code)]
    fn list_units_by_name(
        _sd: &Self::ServiceDiscoveryManager,
        _name_args: &UnitNameDiscoveryArgs,
    ) -> impl Stream<Item = Self::ServiceUnitDefinition> + Send {
        unimplemented!("list_units_by_name unimplemented!");
        stream::empty()
    }

    #[allow(unreachable_code)]
    fn create_untyped_stream(
        _context: Self::ContextRef,
        _id: &Self::UnitIdentifier,
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

    fn get_owning_fqin(_sud: &Self::UnitIdentifier) -> &ServiceFqin {
        unimplemented!("get_owning_fqin unimplemented!")
    }
    fn get_unit_identifier(_sud: &Self::ServiceUnitDefinition) -> Self::UnitIdentifier {
        unimplemented!("get_unit_identifier unimplemented!")
    }
    fn get_unit_type(_sud: &Self::ServiceUnitDefinition) -> UnitType {
        unimplemented!("get_unit_type unimplemented!")
    }
}

pub struct CommsImpl;
impl Comms for CommsImpl {
    type ContextRef = ContextRef;
    type UnitIdentifier = UnitIdentifier;
    type ServiceUnitDefinition = ServiceUnitDefinition;
    type ServiceDiscoveryManager = ServiceDiscoveryManager;

    fn list_units_by_name(
        sd: &Self::ServiceDiscoveryManager,
        name_args: &UnitNameDiscoveryArgs,
    ) -> impl Stream<Item = Self::ServiceUnitDefinition> + Send {
        sd.list_units_by_name(name_args)
    }

    async fn create_untyped_stream(
        context: ContextRef,
        id: &Self::UnitIdentifier,
        duration: Duration,
        options: SubscriberOptions,
    ) -> SdvResult<impl futures::Stream<Item = SdvResult<(u32, std::vec::Vec<u8>)>> + Send + 'static>
    {
        SubscriberStream::create(context, id, duration, options).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use protobuf::well_known_types::timestamp::Timestamp;
    use protobuf::Message;
    use protobuf::MessageField;

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
            _id: &Self::UnitIdentifier,
            _duration: Duration,
            _options: SubscriberOptions,
        ) -> SdvResult<impl Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send + 'static> {
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

    #[tokio::test]
    async fn test_create_hb_stream_is_created_when_dt_stream_is_created() {
        let stream = hb_sud_id_to_stream::<TestableComms>((), &()).await;

        assert!(stream.is_ok());
        let mut stream = stream.unwrap();
        let data = &stream.next().await.unwrap().unwrap()[0];
        assert!(data.timestamp.clone().map(|t| t.seconds) == MessageField::some(HB_TIMESTAMP_S));
        assert!(data.timestamp.clone().map(|t| t.nanos) == MessageField::some(HB_TIMESTAMP_NS));
    }

    #[tokio::test]
    async fn test_create_hb_stream_is_not_created_when_dt_stream_is_not_created() {
        struct CommsDtStreamCreationError;
        impl Comms for CommsDtStreamCreationError {
            type ContextRef = ();
            type UnitIdentifier = ();
            type ServiceUnitDefinition = ();
            type ServiceDiscoveryManager = ();

            async fn create_untyped_stream(
                _context: Self::ContextRef,
                _id: &Self::UnitIdentifier,
                _duration: Duration,
                _options: SubscriberOptions,
            ) -> SdvResult<impl Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send + 'static>
            {
                Err::<stream::Pending<_>, _>(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound))
            }
        }

        let stream = hb_sud_id_to_stream::<CommsDtStreamCreationError>((), &()).await;
        assert!(stream.is_err());
    }
}
