use futures::future;
use futures::stream;
use futures::Future;
use futures::Stream;
use futures::StreamExt;
use sdv_comms::dt::SubscriberOptions;
use sdv_comms::dt::SubscriberStream;
use sdv_comms::id::UnitIdentifier;
use sdv_comms::sd::ServiceUnitChangeEvent;
use sdv_comms::sd::UnitMetadata;
use sdv_comms::sd::UnitNameDiscoveryArgs;
use sdv_comms::ContextRef;
use sdv_comms::{
    id::ServiceFqin,
    sd::{ServiceDiscoveryManager, ServiceUnitDefinition, UnitType},
};
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use std::sync::Arc;
use std::time::Duration;
use tap::Pipe;
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

    async fn subscribe_sud_events_by_type(
        sd: Arc<Self::ServiceDiscoveryManager>,
        unit_type: &UnitType,
    ) -> SdvResult<impl Stream<Item = Event<Self::ServiceUnitDefinition>> + Send + Unpin + 'static>
    {
        sd.subscribe_service_unit_change_by_type(unit_type)
            .await?
            .filter_map(|e: ServiceUnitChangeEvent| {
                match e {
                    ServiceUnitChangeEvent::Registered(sud) => Some(Event::Registered(sud)),
                    ServiceUnitChangeEvent::Unregistered(sud) => Some(Event::Unregistered(sud)),
                    ServiceUnitChangeEvent::Changed(_sud) => None,
                }
                .pipe(future::ready)
            })
            .pipe(Ok)
    }

    fn get_owning_fqin(id: &Self::UnitIdentifier) -> &ServiceFqin {
        id.get_fqin()
    }

    fn get_topic(id: &Self::ServiceUnitDefinition) -> SdvResult<String> {
        let metad = if let UnitMetadata::Publisher(m) = id.get_unit_metadata() {
            m
        } else {
            return Err(SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                "SUD is not of a publisher",
            ));
        };
        Ok(metad.get_topic())
    }

    fn get_unit_name(id: &Self::UnitIdentifier) -> &str {
        id.get_unit_name()
    }

    fn get_unit_identifier(sud: &Self::ServiceUnitDefinition) -> Self::UnitIdentifier {
        sud.get_unit_identifier()
    }

    fn get_unit_type(sud: &Self::ServiceUnitDefinition) -> UnitType {
        sud.get_unit_type()
    }
}

pub enum Event<T> {
    Registered(T),
    Unregistered(T),
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
        unimplemented!("subscribe_sud_registration_by_name unimplemented!");
        stream::empty().pipe(Ok).pipe(future::ready)
    }

    #[allow(unreachable_code)]
    fn subscribe_sud_events_by_type(
        _sd: Arc<Self::ServiceDiscoveryManager>,
        _unit_type: &UnitType,
    ) -> impl Future<
        Output = SdvResult<
            impl Stream<Item = Event<Self::ServiceUnitDefinition>> + Send + Unpin + 'static,
        >,
    > + Send {
        unimplemented!("subscribe_sud_events_by_type unimplemented!");
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

    fn get_unit_name(_id: &Self::UnitIdentifier) -> &str {
        unimplemented!("get_unit_identifier unimplemented!")
    }

    fn get_unit_identifier(_sud: &Self::ServiceUnitDefinition) -> Self::UnitIdentifier {
        unimplemented!("get_unit_identifier unimplemented!")
    }

    fn get_unit_type(_sud: &Self::ServiceUnitDefinition) -> UnitType {
        unimplemented!("get_unit_type unimplemented!")
    }

    fn get_topic(_id: &Self::ServiceUnitDefinition) -> SdvResult<String> {
        unimplemented!("get_topic unimplemented!")
    }
}
