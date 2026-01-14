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

use crate::async_trait;
use crate::hb_config::HeartbeatMonitoringConfig;
use crate::AuthorizationImpl;
use com_sdv_google_health_health_monitor_registration_service_rpc::Interface as HealthMonitorRegistrationService;
use health_api::health_monitor_registration_service::{
    RegisterConfigurationRequest, RegisterConfigurationResponse, UnregisterConfigurationRequest,
    UnregisterConfigurationResponse,
};
use log::{info, warn};
use sdv_comms::id::ServiceFqin;
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::instrument;

#[derive(Debug)]
pub enum HBRegistrationAction {
    RegisterImplicitly(HeartbeatMonitoringConfig),
    RegisterExplicitly(HeartbeatMonitoringConfig, oneshot::Sender<SdvResult<()>>),
    UnregisterImplicitly,
    UnregisterExplicitly(oneshot::Sender<SdvResult<()>>),
}

#[derive(Debug)]
pub struct HBRegistrationCommand {
    pub(crate) fqin: ServiceFqin,
    pub(crate) action: HBRegistrationAction,
}

#[derive(Debug)]
pub enum RegistrationCommandError {
    AlreadyRegistered,
    PublicationNotFound,
}

pub struct HBRegistrationListener {
    registration_commands_tx: mpsc::Sender<HBRegistrationCommand>,
    authorization: AuthorizationImpl,
}

impl HBRegistrationListener {
    pub fn new(
        registration_commands_tx: mpsc::Sender<HBRegistrationCommand>,
        authorization: AuthorizationImpl,
    ) -> Self {
        Self { registration_commands_tx, authorization }
    }
}
#[async_trait]
#[allow(non_snake_case)]
impl HealthMonitorRegistrationService for HBRegistrationListener {
    #[instrument(skip(self))]
    async fn RegisterConfiguration(
        &self,
        fqin: sdv_comms::id::ServiceFqin,
        request: &RegisterConfigurationRequest,
    ) -> sdv_status_rs::SdvResult<RegisterConfigurationResponse> {
        if !self.authorization.is_request_authorized(&fqin) {
            warn!(
                "Request from {fqin} was rejected since it is not authorized to call Health Monitor"
            );
            return Err(SdvStatus::with_message(
                SdvStatusCode::PermissionDenied,
                "Only clients on the same VM may call Health Monitor",
            ));
        }

        let config = match request.config.clone().into_option() {
            Some(x) => x,
            None => {
                return Err(SdvStatus::with_message(
                    SdvStatusCode::InvalidArgument,
                    "config parameter cannot be None",
                ));
            }
        };
        let config = HeartbeatMonitoringConfig::try_from(config)
            .map_err(|err| SdvStatus::with_message(SdvStatusCode::InvalidArgument, err.as_str()))?;

        info!("Queueing up register for fqin:{}", fqin);
        let (tx, rx) = oneshot::channel();
        self.registration_commands_tx
            .send(HBRegistrationCommand {
                fqin,
                action: HBRegistrationAction::RegisterExplicitly(config, tx),
            })
            .await
            .or(Err(SdvStatus::with_message(
                SdvStatusCode::Unknown,
                "Health Reporter Registration Queue unexpectedly closed",
            )))?;
        rx.await
            .or(Err(SdvStatus::with_message(
                SdvStatusCode::Unknown,
                "Registration Command Oneshot Channel unexpectedly closed",
            )))?
            .map(|_| RegisterConfigurationResponse::new())
    }

    #[instrument(skip(self))]
    async fn UnregisterConfiguration(
        &self,
        fqin: sdv_comms::id::ServiceFqin,
        _request: &UnregisterConfigurationRequest,
    ) -> sdv_status_rs::SdvResult<UnregisterConfigurationResponse> {
        info!("Queueing up unregister for fqin:{}", fqin);
        let (tx, rx) = oneshot::channel();
        self.registration_commands_tx
            .send(HBRegistrationCommand {
                fqin,
                action: HBRegistrationAction::UnregisterExplicitly(tx),
            })
            .await
            .map_err(|_| {
                let err = "Could not send deregistration command, queue dropped!";
                warn!("{err}");
                SdvStatus::with_message(SdvStatusCode::Internal, err)
            })?;
        rx.await
            .map_err(|_| {
                let err = "Could not receive deregistration ack command, oneshot dropped!";
                warn!("{err}");
                SdvStatus::with_message(SdvStatusCode::Internal, err)
            })?
            .map(|_| UnregisterConfigurationResponse::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hb_monitor_factory::MockHeartbeatMonitorFactory;
    use crate::monitor_model::test_utils::TestMonitor;
    use health_api::health_config::HealthConfiguration;
    use protobuf::MessageField;
    use std::str::FromStr;
    use std::sync::LazyLock;

    use tokio::sync::mpsc;

    static HEALTH_MONITOR_FQIN: &str = "vm:com.google.sdv.HealthMonitorBundle/sample";
    static HEALTHY_SERVICE_FQIN_STR: &str = "vm:com.google.sdv.HealthyServiceBundle/sample";
    static DEFAULT_VALID_CONFIG: LazyLock<HealthConfiguration> =
        LazyLock::new(|| HealthConfiguration {
            is_heartbeat_monitored: true,
            initial_delay_ms: Some(100),
            period_ms: Some(200),
            num_periods: Some(3),
            task_duration_ms: Some(40),
            special_fields: protobuf::SpecialFields::default(),
        });

    #[tokio::test]
    async fn test_register_configuration_successful() {
        let mut monitor_factory = MockHeartbeatMonitorFactory::new();
        let valid_parsed_config =
            HeartbeatMonitoringConfig::try_from(DEFAULT_VALID_CONFIG.clone()).unwrap();
        let expected_monitor =
            TestMonitor { config: valid_parsed_config, ..TestMonitor::default() };

        monitor_factory.expect_new_monitor().returning(move |_, _| Ok(Box::new(expected_monitor)));
        let auth = AuthorizationImpl::new(ServiceFqin::from_str(HEALTH_MONITOR_FQIN).unwrap());
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let interface = HBRegistrationListener::new(command_tx, auth);

        let request = RegisterConfigurationRequest {
            config: MessageField::some(DEFAULT_VALID_CONFIG.clone()),
            ..Default::default()
        };

        let response = tokio::task::spawn(async move {
            interface
                .RegisterConfiguration(
                    ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                    &request,
                )
                .await
        });
        let monitor_task = tokio::task::spawn(async move {
            match command_rx.recv().await.unwrap() {
                HBRegistrationCommand {
                    fqin,
                    action: HBRegistrationAction::RegisterExplicitly(config, oneshot_tx),
                } => {
                    assert_eq!(fqin.to_string(), HEALTHY_SERVICE_FQIN_STR);
                    assert_eq!(config, valid_parsed_config);

                    oneshot_tx.send(Ok(())).unwrap();
                }
                HBRegistrationCommand {
                    fqin: _,
                    action: HBRegistrationAction::UnregisterExplicitly(_),
                } => {
                    panic!("Unregister command not currently under test")
                }
                _ => panic!("Unexpected command type"),
            }
        });
        monitor_task.await.unwrap();
        let response = response.await.unwrap();

        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_register_error_correctly_propagated_from_health_monitor_to_rpc_response() {
        let mut monitor_factory = MockHeartbeatMonitorFactory::new();
        monitor_factory.expect_new_monitor().returning(|_, _| Ok(Box::new(TestMonitor::default())));
        let auth = AuthorizationImpl::new(ServiceFqin::from_str(HEALTH_MONITOR_FQIN).unwrap());
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let interface = HBRegistrationListener::new(command_tx, auth);

        let request = RegisterConfigurationRequest {
            config: MessageField::some(DEFAULT_VALID_CONFIG.clone()),
            ..Default::default()
        };

        let response = tokio::task::spawn(async move {
            interface
                .RegisterConfiguration(
                    ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                    &request,
                )
                .await
        });
        let monitor_task = tokio::task::spawn(async move {
            match command_rx.recv().await.unwrap() {
                HBRegistrationCommand {
                    fqin: _,
                    action: HBRegistrationAction::RegisterExplicitly(_, oneshot_tx),
                } => {
                    oneshot_tx.send(Err(SdvStatus::new(SdvStatusCode::AlreadyExists))).unwrap();
                }
                HBRegistrationCommand {
                    fqin: _,
                    action: HBRegistrationAction::UnregisterExplicitly(_),
                } => {
                    panic!("Unregister command not currently under test")
                }
                _ => panic!("Unexpected command type"),
            }
        });
        monitor_task.await.unwrap();
        let response = response.await.unwrap();

        assert!(response.is_err());
        assert!(response.unwrap_err().status_code() == SdvStatusCode::AlreadyExists);
    }

    #[tokio::test]
    async fn test_register_configuration_missing_config() {
        let auth = AuthorizationImpl::new(ServiceFqin::from_str(HEALTH_MONITOR_FQIN).unwrap());
        let (command_tx, _) = mpsc::channel(1);
        let interface = HBRegistrationListener::new(command_tx, auth);

        let response = interface
            .RegisterConfiguration(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                &RegisterConfigurationRequest::new(),
            )
            .await;

        assert!(response.is_err());
        assert_eq!(response.unwrap_err().status_code(), SdvStatusCode::InvalidArgument);
    }

    #[tokio::test]
    async fn test_register_configuration_invalid_config() {
        let auth = AuthorizationImpl::new(ServiceFqin::from_str(HEALTH_MONITOR_FQIN).unwrap());
        let (command_tx, _) = mpsc::channel(1);
        let interface = HBRegistrationListener::new(command_tx, auth);

        let request = RegisterConfigurationRequest {
            config: MessageField::some(HealthConfiguration {
                period_ms: Some(0), // Invalid: period_ms must be greater than 0
                ..Default::default()
            }),
            ..Default::default()
        };

        let response = interface
            .RegisterConfiguration(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                &request,
            )
            .await;

        assert!(response.is_err());
        assert_eq!(response.unwrap_err().status_code(), SdvStatusCode::InvalidArgument);
    }

    #[tokio::test]
    async fn test_register_configuration_unauthorized_client() {
        let auth = AuthorizationImpl::new(ServiceFqin::from_str(HEALTH_MONITOR_FQIN).unwrap());
        let (command_tx, _) = mpsc::channel(1);
        let interface = HBRegistrationListener::new(command_tx, auth);

        let request = RegisterConfigurationRequest {
            config: MessageField::some(HealthConfiguration {
                initial_delay_ms: Some(100),
                period_ms: Some(200),
                num_periods: Some(3),
                task_duration_ms: Some(40),
                ..Default::default()
            }),
            ..Default::default()
        };
        // Set the Client FQIN to anything with a different vm name than that of the Health Monitor
        let response = interface
            .RegisterConfiguration(
                ServiceFqin::from_str("different-vm:com.google.sdv.HealthyServiceBundle/sample")
                    .expect("Valid Fqin"),
                &request,
            )
            .await;

        assert_eq!(response.unwrap_err().status_code(), SdvStatusCode::PermissionDenied);
    }

    #[tokio::test]
    async fn test_unregister_configuration_successful() {
        let auth = AuthorizationImpl::new(ServiceFqin::from_str(HEALTH_MONITOR_FQIN).unwrap());
        let (command_tx, mut command_rx) = mpsc::channel(1);
        let interface = HBRegistrationListener::new(command_tx, auth);

        let monitor_task = tokio::task::spawn(async move {
            match command_rx.recv().await.unwrap() {
                HBRegistrationCommand {
                    fqin: _,
                    action: HBRegistrationAction::RegisterExplicitly(_, _),
                } => {
                    panic!("Register command not currently under test")
                }
                HBRegistrationCommand {
                    fqin: _,
                    action: HBRegistrationAction::UnregisterExplicitly(oneshot_tx),
                } => {
                    oneshot_tx.send(Ok(())).unwrap();
                }
                _ => panic!("Unexpected command type"),
            }
        });

        let response = tokio::task::spawn(async move {
            interface
                .UnregisterConfiguration(
                    ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                    &UnregisterConfigurationRequest::new(),
                )
                .await
        });

        monitor_task.await.unwrap();
        let response = response.await.unwrap();
        assert!(response.is_ok());
    }
}
