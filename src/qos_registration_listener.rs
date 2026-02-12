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
#![allow(dead_code)]

use crate::commstack_trait::{Comms, Event};
use crate::hb_implicit_monitoring_db::ImplicitMonitoringConfigRepo;

use futures::StreamExt;
use health_api::health_config::QosMonitoringConfiguration;
use log::error;
use sdv_comms::sd::UnitType;
use sdv_status_rs::SdvStatusCode;
use sdv_status_rs::{SdvResult, SdvStatus};
use std::sync::Arc;
use tokio::sync::mpsc;

pub enum QoSRegistrationAction<T: Comms> {
    Register(T::ServiceUnitDefinition, QosMonitoringConfiguration),
    Unregister(T::ServiceUnitDefinition),
}

pub struct QosRegistrationListener {
    sud_listening_task: Option<tokio::task::JoinHandle<SdvResult<()>>>,
}

impl QosRegistrationListener {
    pub fn new<T: Comms>(
        sd: Arc<T::ServiceDiscoveryManager>,
        config_db: ImplicitMonitoringConfigRepo,
    ) -> (Self, mpsc::Receiver<QoSRegistrationAction<T>>) {
        let (sender, receiver) = mpsc::channel(100);
        let sud_listening_task =
            Some(tokio::spawn(listen_to_sud_events_and_send_commands(sd, sender, config_db)));
        (Self { sud_listening_task }, receiver)
    }
}

async fn listen_to_sud_events_and_send_commands<T: Comms>(
    sd: Arc<T::ServiceDiscoveryManager>,
    sender: mpsc::Sender<QoSRegistrationAction<T>>,
    config_db: ImplicitMonitoringConfigRepo,
) -> SdvResult<()> {
    let qos_unit_type = build_qos_hb_unit_type()?;
    T::subscribe_sud_events_by_type(sd, &qos_unit_type)
        .await?
        .for_each(async |event| {
            send_qos_registration_command(event, &sender, &config_db).await.unwrap_or_else(
                move |e| error!("Error in creating QoS monitoring regiastration commands: {e:?}"),
            )
        })
        .await;
    Ok(())
}

async fn send_qos_registration_command<T: Comms>(
    event: Event<T::ServiceUnitDefinition>,
    sender: &mpsc::Sender<QoSRegistrationAction<T>>,
    config_db: &ImplicitMonitoringConfigRepo,
) -> SdvResult<()> {
    let sud = match event {
        Event::Registered(ref sud) => sud,
        Event::Unregistered(ref sud) => sud,
    };
    let id = T::get_unit_identifier(sud);
    let topic = T::get_topic(sud)?;
    let fqin = T::get_owning_fqin(&id);

    let config = config_db.get_qos_config(fqin).await?.and_then(|r| r.get(&topic)).ok_or(
        SdvStatus::with_message(
            SdvStatusCode::NotFound,
            "QoS config not registered with SBR for this topic:{topic}",
        ),
    )?;

    let command = match event {
        Event::Registered(sud) => QoSRegistrationAction::Register(sud, config.clone()),
        Event::Unregistered(sud) => QoSRegistrationAction::Unregister(sud),
    };

    sender.send(command).await.map_err(|e| {
        SdvStatus::with_message(
            SdvStatusCode::Internal,
            format!("qos command channel error: {e:?}"),
        )
    })
}

impl Drop for QosRegistrationListener {
    fn drop(&mut self) {
        if let Some(task) = self.sud_listening_task.take() {
            task.abort();
        }
    }
}

fn build_qos_hb_unit_type() -> SdvResult<UnitType> {
    const PACKAGE: &str = "com.sdv.google.health";
    const BUNDLE: &str = "HealthMonitorServiceBundle";
    const TYPE_NAME: &str = "QualityOfServiceHeartbeat";
    UnitType::builder()
        .set_sdv_package_name(PACKAGE)
        .set_service_bundle_name(BUNDLE)
        .set_unit_type_name(TYPE_NAME)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hb_implicit_monitoring_db::InstanceConfig;
    use futures::stream::Stream;
    use sdv_comms::id::ServiceFqin;
    use std::collections::HashMap;
    use std::str::FromStr;
    use std::sync::Mutex;
    use tokio_stream::wrappers::ReceiverStream;

    #[derive(Clone)]
    struct SUD {
        fqin: ServiceFqin,
        unit_name: String,
        topic: String,
        unit_type: UnitType,
    }

    struct TestComms;
    impl Comms for TestComms {
        type ContextRef = ();
        type UnitIdentifier = (ServiceFqin, String);
        type ServiceUnitDefinition = SUD;
        type ServiceDiscoveryManager =
            Mutex<Option<std::pin::Pin<Box<dyn Stream<Item = Event<SUD>> + Send>>>>;

        fn get_owning_fqin(id: &Self::UnitIdentifier) -> &ServiceFqin {
            &id.0
        }
        fn get_unit_name(id: &Self::UnitIdentifier) -> &str {
            &id.1
        }
        fn get_unit_identifier(sud: &Self::ServiceUnitDefinition) -> Self::UnitIdentifier {
            (sud.fqin.clone(), sud.unit_name.clone())
        }
        fn get_unit_type(sud: &Self::ServiceUnitDefinition) -> UnitType {
            sud.unit_type.clone()
        }

        fn get_topic(sud: &Self::ServiceUnitDefinition) -> SdvResult<String> {
            Ok(sud.topic.clone())
        }

        async fn subscribe_sud_events_by_type(
            sd: Arc<Self::ServiceDiscoveryManager>,
            _unit_type: &UnitType,
        ) -> SdvResult<impl Stream<Item = Event<Self::ServiceUnitDefinition>> + Send + 'static>
        {
            let mut guard = sd.lock().unwrap();
            Ok(guard.take().expect("Stream already taken"))
        }
    }

    #[tokio::test]
    async fn test_send_qos_registration_command_registered() {
        let fqin = ServiceFqin::from_str("vm:com.pkg.Bundle/instance").unwrap();
        let topic = "my-topic";
        let unit_name = "my-unit";
        let qos_config = QosMonitoringConfiguration::default();

        let qos_map = HashMap::from([(topic.to_string(), qos_config.clone())]);
        let config_db = ImplicitMonitoringConfigRepo::new_for_testing(HashMap::from([(
            fqin.clone(),
            InstanceConfig { hb_config: None, qos_config: qos_map },
        )]));

        let (tx, mut rx) = mpsc::channel(1);
        let sud = SUD {
            fqin: fqin.clone(),
            unit_name: unit_name.to_string(),
            topic: topic.to_string(),
            unit_type: build_qos_hb_unit_type().unwrap(),
        };
        let event = Event::Registered(sud);

        send_qos_registration_command::<TestComms>(event, &tx, &config_db).await.unwrap();

        let action = rx.recv().await.unwrap();
        match action {
            QoSRegistrationAction::Register(sud, _) => {
                assert_eq!(sud.topic, topic);
            }
            _ => panic!("Expected Register action"),
        }
    }

    #[tokio::test]
    async fn test_send_qos_registration_command_unregistered() {
        let fqin = ServiceFqin::from_str("vm:com.pkg.Bundle/instance").unwrap();
        let topic = "my-topic";
        let unit_name = "my-unit";
        let qos_config = QosMonitoringConfiguration::default();

        let qos_map = HashMap::from([(topic.to_string(), qos_config.clone())]);
        let config_db = ImplicitMonitoringConfigRepo::new_for_testing(HashMap::from([(
            fqin.clone(),
            InstanceConfig { hb_config: None, qos_config: qos_map },
        )]));

        let (tx, mut rx) = mpsc::channel(1);
        let sud = SUD {
            fqin: fqin.clone(),
            unit_name: unit_name.to_string(),
            topic: topic.to_string(),
            unit_type: build_qos_hb_unit_type().unwrap(),
        };

        let event = Event::Unregistered(sud);

        send_qos_registration_command::<TestComms>(event, &tx, &config_db).await.unwrap();

        let action = rx.recv().await.unwrap();
        match action {
            QoSRegistrationAction::Unregister(sud) => {
                assert_eq!(sud.topic, topic);
            }
            _ => panic!("Expected Unregister action"),
        }
    }

    #[tokio::test]
    async fn test_send_qos_registration_command_config_not_found() {
        let fqin = ServiceFqin::from_str("vm:com.pkg.Bundle/instance").unwrap();
        let topic = "my-topic";
        let unit_name = "my-unit";

        let config_db = ImplicitMonitoringConfigRepo::new_for_testing(HashMap::from([(
            fqin.clone(),
            InstanceConfig { hb_config: None, qos_config: HashMap::new() },
        )]));

        let (tx, _rx) = mpsc::channel(1);
        let sud = SUD {
            fqin: fqin.clone(),
            unit_name: unit_name.to_string(),
            topic: topic.to_string(),
            unit_type: build_qos_hb_unit_type().unwrap(),
        };

        let event = Event::Registered(sud);

        let res = send_qos_registration_command::<TestComms>(event, &tx, &config_db).await;

        assert!(res.is_err());
        assert_eq!(res.unwrap_err().status_code(), SdvStatusCode::NotFound);
    }

    #[tokio::test]
    async fn test_qos_registration_listener_constructor_to_output_channel() {
        let fqin = ServiceFqin::from_str("vm:com.pkg.Bundle/instance").unwrap();
        let topic = "my-topic";
        let unit_name = "my-unit";
        let qos_config = QosMonitoringConfiguration::default();

        let qos_map = HashMap::from([(topic.to_string(), qos_config.clone())]);

        let config_db = ImplicitMonitoringConfigRepo::new_for_testing(HashMap::from([(
            fqin.clone(),
            InstanceConfig { hb_config: None, qos_config: qos_map },
        )]));

        let sud = SUD {
            fqin: fqin.clone(),
            topic: topic.to_string(),
            unit_name: unit_name.to_string(),
            unit_type: build_qos_hb_unit_type().unwrap(),
        };

        let (tx_events, rx_events) = mpsc::channel(1);
        let event_stream = ReceiverStream::new(rx_events).boxed();
        let sd = Arc::new(Mutex::new(Some(event_stream)));

        let (_listener, mut rx_actions) = QosRegistrationListener::new::<TestComms>(sd, config_db);

        tx_events.send(Event::Registered(sud)).await.unwrap();

        let action = rx_actions.recv().await.unwrap();
        match action {
            QoSRegistrationAction::Register(sud, _) => {
                assert_eq!(sud.topic, topic);
            }
            _ => panic!("Expected Register action"),
        }
    }

    #[tokio::test]
    async fn test_build_qos_hb_unit_type_does_not_fail() {
        build_qos_hb_unit_type().unwrap();
    }
}
