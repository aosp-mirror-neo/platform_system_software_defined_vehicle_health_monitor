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

use binder::BinderFeatures;
use binder::ExceptionCode;
use binder::Interface;
use binder::Status;
use binder::Strong;
use google_sdv_orchestration::aidl::google::sdv::orchestration::IOrchestrationAgent::BpOrchestrationAgent;
use google_sdv_orchestration::aidl::google::sdv::orchestration::IOrchestrationAgent::IOrchestrationAgent;
use google_sdv_orchestration::aidl::google::sdv::orchestration::ServiceBundleInstanceRecoveryState::ServiceBundleInstanceRecoveryState;
use google_sdv_orchestration::aidl::google::sdv::orchestration::ServiceBundleInstanceLifecycleState::ServiceBundleInstanceLifecycleState;
use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::BnServiceBundleInstanceStateChangeListener;
use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::IServiceBundleInstanceStateChangeListener;
use google_sdv_identity::aidl::google::sdv::identity::ServiceFqin::ServiceFqin as AidlServiceFqin;
use log::{error, info};
use persistent_binder::{BinderConnectionManager, PersistentBinderConnection};
use sdv_comms::id::ServiceFqin;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::sb_recovery_monitor::SBLifecycleState;
use crate::sb_recovery_monitor::SBOrchestrationState;
use crate::sb_recovery_monitor::SBRecoveryState;

/// Represents a change in a service bundle's instance state.
#[derive(Debug, Clone)]
pub struct ServiceBundleRecoveryEvent {
    pub fqin: ServiceFqin,
    pub state: SBOrchestrationState,
}

pub struct SbRecoveryEventSourceImpl {
    orchestrator_connection_setup_task:
        tokio::task::JoinHandle<PersistentBinderConnection<dyn IOrchestrationAgent>>,
}

/// Blocking function to get the Orchestrator binder service.
fn get_orchestrator_binder_service() -> Strong<dyn IOrchestrationAgent> {
    let descriptor =
        <BpOrchestrationAgent as IOrchestrationAgent>::get_descriptor().to_owned() + "/default";

    loop {
        match binder::wait_for_interface(&descriptor) {
            Ok(orchestrator_agent) => return orchestrator_agent,
            Err(e) => error!("Failed to get IOrchestrationAgent interface, retrying: {e:?}"),
        }
    }
}

fn subscribe_to_recovery_events(
    orchestrator_agent_service: &Strong<dyn IOrchestrationAgent>,
    recovery_event_listener: &Strong<dyn IServiceBundleInstanceStateChangeListener>,
) {
    let result = orchestrator_agent_service
        .registerServiceBundleInstanceStateChangeListener(recovery_event_listener);

    if let Err(e) = result {
        error!("Error subscribing to ServiceBundleInstanceRecoveryState changes: {e}");
    }
}

/// Adds a `listener` to the binder. Also adds a reconnect listener to the `orchestrator_connection`,
/// which re-adds the listener to a new binder instance in case the previous one has crashed.
/// New binder creation managed internally, by `orchestrator_connection`
/// Ownership: post function call, orchestrator_connection holds references to both `listener` and
/// `subscribe_to_recovery_events` structs
fn connect_persistently_to_orchestrator(
    orchestrator_connection: &impl BinderConnectionManager<dyn IOrchestrationAgent>,
    listener: Strong<dyn IServiceBundleInstanceStateChangeListener>,
    subscribe_to_recovery_events: impl Fn(&Strong<dyn IOrchestrationAgent>, &Strong<dyn IServiceBundleInstanceStateChangeListener>)
        + Send
        + Sync
        + 'static,
) {
    {
        let orchestrator_binder = orchestrator_connection
            .get_binder()
            .lock()
            .expect("Lock can't be poisoned, as PersistentBinderConnection accessed from 1 thread");

        subscribe_to_recovery_events(&orchestrator_binder, &listener);
    };

    orchestrator_connection.set_reconnect_listener(Box::new(move |orchestrator_binder| {
        info!("Re-subscribing to ServiceBundleInstanceRecoveryState changes");
        subscribe_to_recovery_events(orchestrator_binder, &listener);
    }));
}

// We expect less than 100 bundles changing state at instant in time:
const SB_EVENTS_BUFFER_SIZE: usize = 100;

impl SbRecoveryEventSourceImpl {
    pub fn setup(vm_name: &'static str) -> (Self, mpsc::Receiver<ServiceBundleRecoveryEvent>) {
        let (tx, rx) = mpsc::channel(SB_EVENTS_BUFFER_SIZE);
        (
            Self {
                orchestrator_connection_setup_task: tokio::task::spawn_blocking(move || {
                    let persistent_connecter = Arc::into_inner(PersistentBinderConnection::new(
                        get_orchestrator_binder_service,
                    ))
                    .expect("We have just created this Arc, this is the only reference");

                    connect_persistently_to_orchestrator(
                        &persistent_connecter,
                        BnServiceBundleInstanceStateChangeListener::new_binder(
                            RecoveryEventCallback { tx, vm_name },
                            BinderFeatures::default(),
                        ),
                        subscribe_to_recovery_events,
                    );
                    persistent_connecter
                }),
            },
            rx,
        )
    }
}

impl Drop for SbRecoveryEventSourceImpl {
    fn drop(&mut self) {
        self.orchestrator_connection_setup_task.abort();
    }
}

struct RecoveryEventCallback {
    tx: mpsc::Sender<ServiceBundleRecoveryEvent>,
    vm_name: &'static str,
}

impl Interface for RecoveryEventCallback {}

#[allow(non_snake_case)]
impl IServiceBundleInstanceStateChangeListener for RecoveryEventCallback {
    fn onServiceBundleInstanceStateChanged(
        &self,
        aidl_fqin: &AidlServiceFqin,
        new_recovery_state: ServiceBundleInstanceRecoveryState,
        expected_lifecycle_state: ServiceBundleInstanceLifecycleState,
    ) -> binder::Result<()> {
        let event = ServiceBundleRecoveryEvent {
            fqin: ServiceFqin::from_aidl(aidl_fqin, self.vm_name),
            state: SBOrchestrationState::from(&new_recovery_state, &expected_lifecycle_state)?,
        };
        self.tx.blocking_send(event).map_err(|err| {
            error!("Unexpected consumer of SB recovery events dropped! {err}");
            Status::new_exception_str(
                ExceptionCode::ILLEGAL_STATE,
                Some("mpsc processing SB events dropped!"),
            )
        })
    }
}

trait FromAidlFqin {
    fn from_aidl(aidl_fqin: &AidlServiceFqin, vm_name: &str) -> Self;
}

impl FromAidlFqin for ServiceFqin {
    fn from_aidl(aidl_fqin: &AidlServiceFqin, vm_name: &str) -> Self {
        ServiceFqin::builder()
            .sdv_vm_name(vm_name)
            .sdv_package_name(&aidl_fqin.sdvPackageName)
            .service_bundle_name(&aidl_fqin.serviceBundleName)
            .service_instance_name(&aidl_fqin.serviceInstanceName)
            .build()
            .expect("Converting validated AIDL fqin struct to ServiceFqin")
    }
}

impl SBOrchestrationState {
    fn from(
        aidl_recovery_state: &ServiceBundleInstanceRecoveryState,
        aidl_lifecycle_state: &ServiceBundleInstanceLifecycleState,
    ) -> Result<Self, binder::Status> {
        let recovery_state = match aidl_recovery_state {
            &ServiceBundleInstanceRecoveryState::OPERATIONAL => Ok(SBRecoveryState::Normal),
            &ServiceBundleInstanceRecoveryState::RETRYING => Ok(SBRecoveryState::Recovering),
            &ServiceBundleInstanceRecoveryState::RETRY_FAILED => {
                Ok(SBRecoveryState::FailedRecovery)
            }
            bad_value => Err(Status::new_exception_str(
                ExceptionCode::ILLEGAL_STATE,
                Some(format!("Illegal enum value received from orch binder: {:?}", bad_value)),
            )),
        }?;

        let lifecycle_state = match aidl_lifecycle_state {
            &ServiceBundleInstanceLifecycleState::CREATED => Ok(SBLifecycleState::Created),
            &ServiceBundleInstanceLifecycleState::STARTED => Ok(SBLifecycleState::Started),
            &ServiceBundleInstanceLifecycleState::DESTROYED => Ok(SBLifecycleState::Destroyed),
            bad_value => Err(Status::new_exception_str(
                ExceptionCode::ILLEGAL_STATE,
                Some(format!("Illegal enum value received from orch binder: {:?}", bad_value)),
            )),
        }?;

        Ok(SBOrchestrationState { recovery_state, lifecycle_state })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IOrchestrationAgent::MockIOrchestrationAgent;
    use google_sdv_orchestration::aidl::google::sdv::orchestration::IServiceBundleInstanceStateChangeListener::MockIServiceBundleInstanceStateChangeListener;
    use persistent_binder::MockBinderConnectionManager;
    use std::sync::Mutex;

    #[test]
    fn test_subscribe_to_recovery_events_registers_listener() {
        let mut mock_orchestrator_agent = MockIOrchestrationAgent::new();
        mock_orchestrator_agent
            .expect_registerServiceBundleInstanceStateChangeListener()
            .times(1)
            .returning(|_| Ok(()));
        let mock_orchestrator_agent =
            Strong::new(Box::new(mock_orchestrator_agent) as Box<dyn IOrchestrationAgent>);

        let mock_listener = MockIServiceBundleInstanceStateChangeListener::new();
        let mock_listener = Strong::new(
            Box::new(mock_listener) as Box<dyn IServiceBundleInstanceStateChangeListener>
        );

        subscribe_to_recovery_events(&mock_orchestrator_agent, &mock_listener);
    }

    #[test]
    fn test_connection_to_orchestrator_is_persistent() {
        // SETUP
        let mock_binder = Arc::new(Mutex::new(Strong::new(
            Box::new(MockIOrchestrationAgent::new()) as Box<dyn IOrchestrationAgent>,
        )));

        let mut mock_orchestrator_connection = MockBinderConnectionManager::new();
        mock_orchestrator_connection.expect_get_binder().times(1).return_const(mock_binder);

        let reconnect_listener_capture = Arc::new(Mutex::new(None));
        let reconnect_listener_capture_clone = Arc::clone(&reconnect_listener_capture);
        mock_orchestrator_connection.expect_set_reconnect_listener().times(1).returning(move |f| {
            *reconnect_listener_capture_clone.lock().unwrap() = Some(f);
        });

        let nr_calls_to_subscribe = Arc::new(Mutex::new(0));
        let subscribing_logic_mock = {
            let nr_calls_to_subscribe = nr_calls_to_subscribe.clone();
            move |_: &Strong<dyn IOrchestrationAgent>,
                  _: &Strong<dyn IServiceBundleInstanceStateChangeListener>| {
                *nr_calls_to_subscribe.lock().unwrap() += 1;
            }
        };
        let mock_listener =
            Strong::new(Box::new(MockIServiceBundleInstanceStateChangeListener::new())
                as Box<dyn IServiceBundleInstanceStateChangeListener>);

        // ACT
        connect_persistently_to_orchestrator(
            &mock_orchestrator_connection,
            mock_listener,
            subscribing_logic_mock,
        );

        //ASSERT
        // assert that when calling the fn, subscription logic is executed once,
        // establishing the initial connection:
        assert!(*nr_calls_to_subscribe.lock().unwrap() == 1);

        // assert that fn has also added subscription logic as the reconnect listener. Do this by
        // calling the captured reconnect_listener, and checking whether subscribe logic was rerun
        let dummy_binder =
            Strong::new(Box::new(MockIOrchestrationAgent::new()) as Box<dyn IOrchestrationAgent>);
        reconnect_listener_capture.lock().unwrap().as_ref().unwrap()(&dummy_binder);
        assert!(*nr_calls_to_subscribe.lock().unwrap() == 2);
    }

    #[tokio::test]
    async fn test_recovery_event_callback_on_service_bundle_instance_state_changed() {
        let (tx, mut rx) = mpsc::channel(1);
        let expected_vm_name = "correct-vm";
        let callback = RecoveryEventCallback { tx, vm_name: expected_vm_name };

        let aidl_fqin = AidlServiceFqin {
            sdvVmName: "wrong-vm-from-binder".to_string(),
            sdvPackageName: "com.android.sdv.sample.oem.health.monitored".to_string(),
            serviceBundleName: "SampleHMBundle".to_string(),
            serviceInstanceName: "test1".to_string(),
        };

        {
            let aidl_fqin = aidl_fqin.clone();
            tokio::task::spawn_blocking(move || {
                callback
                    .onServiceBundleInstanceStateChanged(
                        &aidl_fqin,
                        ServiceBundleInstanceRecoveryState::OPERATIONAL,
                        ServiceBundleInstanceLifecycleState::CREATED,
                    )
                    .unwrap()
            });
        }

        let received_event = rx.recv().await.unwrap();
        assert_eq!(received_event.fqin.get_sdv_vm_name(), expected_vm_name);
        assert_ne!(received_event.fqin.get_sdv_vm_name(), aidl_fqin.sdvVmName);
        assert_eq!(received_event.state.recovery_state, SBRecoveryState::Normal);
    }

    #[test]
    fn test_sborchestrationstate_from_all_states() {
        // Test valid conversions
        let operational_created = SBOrchestrationState::from(
            &ServiceBundleInstanceRecoveryState::OPERATIONAL,
            &ServiceBundleInstanceLifecycleState::CREATED,
        )
        .unwrap();
        assert_eq!(operational_created.recovery_state, SBRecoveryState::Normal);
        assert_eq!(operational_created.lifecycle_state, SBLifecycleState::Created);

        let retrying_started = SBOrchestrationState::from(
            &ServiceBundleInstanceRecoveryState::RETRYING,
            &ServiceBundleInstanceLifecycleState::STARTED,
        )
        .unwrap();
        assert_eq!(retrying_started.recovery_state, SBRecoveryState::Recovering);
        assert_eq!(retrying_started.lifecycle_state, SBLifecycleState::Started);

        let failed_destroyed = SBOrchestrationState::from(
            &ServiceBundleInstanceRecoveryState::RETRY_FAILED,
            &ServiceBundleInstanceLifecycleState::DESTROYED,
        )
        .unwrap();
        assert_eq!(failed_destroyed.recovery_state, SBRecoveryState::FailedRecovery);
        assert_eq!(failed_destroyed.lifecycle_state, SBLifecycleState::Destroyed);

        // Test invalid recovery state
        assert!(SBOrchestrationState::from(
            &ServiceBundleInstanceRecoveryState::SERVICE_BUNDLE_INSTANCE_RECOVERY_STATE_UNSPECIFIED,
            &ServiceBundleInstanceLifecycleState::CREATED
        )
        .is_err());
        assert!(SBOrchestrationState::from(
            &ServiceBundleInstanceRecoveryState(99),
            &ServiceBundleInstanceLifecycleState::CREATED
        )
        .is_err());

        // Test invalid lifecycle state
        assert!(SBOrchestrationState::from(
            &ServiceBundleInstanceRecoveryState::OPERATIONAL,
            &ServiceBundleInstanceLifecycleState::SERVICE_BUNDLE_INSTANCE_LIFECYCLE_STATE_UNSPECIFIED
        )
        .is_err());
        assert!(SBOrchestrationState::from(
            &ServiceBundleInstanceRecoveryState::OPERATIONAL,
            &ServiceBundleInstanceLifecycleState(99)
        )
        .is_err());
    }
}
