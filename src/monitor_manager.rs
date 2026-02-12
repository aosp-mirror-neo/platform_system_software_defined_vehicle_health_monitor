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

use futures::StreamExt;
use log::error;
use sdv_comms::id::ServiceFqin;
use sdv_dump::DumpStateReporter;
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use sdv_status_rs::SdvStatusCode;
use std::collections::HashMap;
use std::io::Error;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

use crate::hb_config::HeartbeatMonitoringConfig;
use crate::hb_explicit_registration::HBRegistrationAction;
use crate::hb_explicit_registration::HBRegistrationCommand;
use crate::hb_implicit_monitoring_db::ImplicitMonitoringConfigRepo;
use crate::hb_monitor_factory::HeartbeatMonitorFactory;
use crate::monitor_model::AgentName;
use crate::monitor_model::EntityMonitor;
use crate::sb_recovery_listener::ServiceBundleRecoveryEvent;
use crate::sb_recovery_monitor::SBLifecycleState;
use crate::sb_recovery_monitor::SBOrchestrationState;
use crate::sb_recovery_monitor::SBRecoveryMonitor;
use crate::sb_recovery_monitor::SBRecoveryState;
use crate::utils;

pub struct MonitorManager {
    sb_recovery_events_processing: JoinHandle<()>,
    hb_registration_commands_processing: JoinHandle<()>,
    pub sb_recovery_monitors: watch::Receiver<HashMap<ServiceFqin, SBRecoveryMonitor>>,
    //TODO: enhance agent crash monitoring with recovery monitoring capabilities
    pub agent_crash_monitors: HashMap<AgentName, Box<dyn EntityMonitor>>,
    pub hb_monitors: watch::Receiver<HashMap<ServiceFqin, Box<dyn EntityMonitor>>>,
}
impl MonitorManager {
    pub fn new(
        implicit_monitoring_config_db: ImplicitMonitoringConfigRepo,
        hb_registration_commands: mpsc::Receiver<HBRegistrationCommand>,
        hb_registration_commands_sink: mpsc::Sender<HBRegistrationCommand>,
        sb_recovery_events: mpsc::Receiver<ServiceBundleRecoveryEvent>,
        agent_crash_monitors: HashMap<AgentName, Box<dyn EntityMonitor>>,
        hb_monitor_factory: impl HeartbeatMonitorFactory + 'static,
    ) -> Self {
        let (sb_recovery_tx, sb_recovery_monitors) = watch::channel(HashMap::new());
        let sb_recovery_events_processing = tokio::spawn(Self::process_recovery_events(
            implicit_monitoring_config_db,
            sb_recovery_events,
            sb_recovery_tx,
            hb_registration_commands_sink,
        ));

        let (hb_monitors_tx, hb_monitors) = watch::channel(HashMap::new());
        let hb_registration_commands_processing =
            tokio::spawn(Self::process_hb_registration_queue(
                hb_registration_commands,
                hb_monitors_tx,
                hb_monitor_factory,
            ));

        Self {
            sb_recovery_events_processing,
            hb_registration_commands_processing,
            sb_recovery_monitors,
            agent_crash_monitors,
            hb_monitors,
        }
    }

    async fn create_implicit_hb_registration_command(
        incoming_event: ServiceBundleRecoveryEvent,
        current_state: Option<SBOrchestrationState>,
        config_db: &ImplicitMonitoringConfigRepo,
    ) -> Option<HBRegistrationCommand> {
        let current_lifecycle_state = current_state.map(|s| s.lifecycle_state);
        match (
            current_lifecycle_state,
            incoming_event.state.lifecycle_state,
            incoming_event.state.recovery_state,
        ) {
            (current, SBLifecycleState::Started, _)
                if current != Some(SBLifecycleState::Started) =>
            {
                config_db
                    .get_implicit_hb_config(&incoming_event.fqin)
                    .await
                    .unwrap_or_else(|e| {
                        error!("Error {e:?} in implicit config retrieval");
                        None
                    })
                    .map(HBRegistrationAction::RegisterImplicitly)
            }
            (
                Some(SBLifecycleState::Started),
                SBLifecycleState::Created | SBLifecycleState::Destroyed,
                SBRecoveryState::Normal,
            ) => config_db
                .get_implicit_hb_config(&incoming_event.fqin)
                .await
                .unwrap_or_else(|e| {
                    error!("Error {e:?} in implicit config retrieval");
                    None
                })
                .map(|_c| HBRegistrationAction::UnregisterImplicitly),
            _ => None,
        }
        .map(|action| HBRegistrationCommand { fqin: incoming_event.fqin, action })
    }

    async fn process_recovery_events(
        implicit_monitoring_config_db: ImplicitMonitoringConfigRepo,
        incoming_events: mpsc::Receiver<ServiceBundleRecoveryEvent>,
        recovery_monitors_sink: watch::Sender<HashMap<ServiceFqin, SBRecoveryMonitor>>,
        hb_registration_commands_sink: mpsc::Sender<HBRegistrationCommand>,
    ) {
        ReceiverStream::new(incoming_events)
            .map(|incoming_event| {
                let current_state =
                    recovery_monitors_sink.borrow().get(&incoming_event.fqin).copied();
                (incoming_event, current_state)
            })
            .inspect(|(incoming, current)| {
                if Some(incoming.state) != *current {
                    recovery_monitors_sink.send_modify(|monitors| {
                        utils::insert(monitors, &incoming.fqin, incoming.state);
                    });
                }
            })
            .filter_map(|(incoming, current)| {
                MonitorManager::create_implicit_hb_registration_command(
                    incoming,
                    current,
                    &implicit_monitoring_config_db,
                )
            })
            .for_each(async |command| {
                hb_registration_commands_sink.send(command).await.unwrap_or_else(|e| {
                    error!("Recovery events processing, channel unexpectedly dropped! Error: {e}")
                });
            })
            .await;
    }

    async fn process_hb_registration_queue(
        mut registration_requests: mpsc::Receiver<HBRegistrationCommand>,
        monitors: watch::Sender<HashMap<ServiceFqin, Box<dyn EntityMonitor>>>,
        monitor_factory: impl HeartbeatMonitorFactory,
    ) {
        while let Some(HBRegistrationCommand { fqin, action }) = registration_requests.recv().await
        {
            let res = match action {
                HBRegistrationAction::RegisterImplicitly(config)
                | HBRegistrationAction::RegisterExplicitly(config, _) => {
                    process_registration_for_hb_monitoring(
                        &monitor_factory,
                        config,
                        fqin,
                        &monitors,
                    )
                }
                HBRegistrationAction::UnregisterExplicitly(_)
                | HBRegistrationAction::UnregisterImplicitly => {
                    process_deregistration_for_hb_monitoring(&fqin, &monitors)
                }
            };
            match action {
                HBRegistrationAction::RegisterExplicitly(_, tx)
                | HBRegistrationAction::UnregisterExplicitly(tx) => {
                    tx.send(res).unwrap_or_else(|_| {
                        error!("Explicit HB registration/deregistration, response channel dropped")
                    });
                }
                HBRegistrationAction::RegisterImplicitly(_)
                | HBRegistrationAction::UnregisterImplicitly => res.unwrap_or_else(|e| {
                    error!("Implicit HB registration/deregistration, error: {}", e);
                }),
            }
        }
        error!("HB Registration channel unexpectedly closed!");
    }
}

pub fn process_registration_for_hb_monitoring(
    monitor_factory: &impl HeartbeatMonitorFactory,
    config: HeartbeatMonitoringConfig,
    fqin: ServiceFqin,
    hb_monitors: &watch::Sender<HashMap<ServiceFqin, Box<dyn EntityMonitor>>>,
) -> SdvResult<()> {
    let m = monitor_factory.new_monitor(&fqin, config)?;
    hb_monitors.send_modify(|hb_monitors| {
        hb_monitors.insert(fqin, m);
    });
    Ok(())
}

pub fn process_deregistration_for_hb_monitoring(
    fqin: &ServiceFqin,
    hb_monitors: &watch::Sender<HashMap<ServiceFqin, Box<dyn EntityMonitor>>>,
) -> SdvResult<()> {
    hb_monitors.borrow();
    let modified = hb_monitors.send_if_modified(|hb_monitors| hb_monitors.remove(fqin).is_some());
    if modified {
        Ok(())
    } else {
        Err(SdvStatus::with_message(
            SdvStatusCode::NotFound,
            "Monitor not found to remove. Check for double explicit deregistration, or \
            explicit deregistration when implicit monitoring was configured",
        ))
    }
}

impl DumpStateReporter for MonitorManager {
    fn dump(&self, writer: &mut dyn std::io::Write, args: &[&std::ffi::CStr]) -> Result<(), Error> {
        writeln!(writer, "\nHEARTBEAT MONITORING:")?;
        if self.hb_monitors.borrow().is_empty() {
            writeln!(writer, "NO ACTIVE MONITORS")?;
        }
        self.hb_monitors.borrow().iter().enumerate().try_for_each(
            |(i, (id, monitor))| -> Result<(), Error> {
                write!(
                    writer,
                    "MONITOR {i}:\n\
                    ID: FQIN: {id}\n"
                )?;
                monitor.dump(writer, args)?;
                Result::Ok(())
            },
        )?;

        writeln!(writer, "\nRECOVERY MONITORING:")?;
        writeln!(writer, "a. Agent monitoring:")?;
        if self.agent_crash_monitors.is_empty() {
            writeln!(writer, "NO ACTIVE MONITORS")?;
        }
        self.agent_crash_monitors.iter().enumerate().try_for_each(
            |(i, (id, monitor))| -> Result<(), Error> {
                write!(
                    writer,
                    "MONITOR {i}:\n\
                    ID: Agent: {id}\n"
                )?;
                monitor.dump(writer, args)?;
                Result::Ok(())
            },
        )?;

        writeln!(writer, "b. SB monitoring:")?;
        if self.sb_recovery_monitors.borrow().is_empty() {
            writeln!(writer, "NO ACTIVE MONITORS")?;
        }
        self.sb_recovery_monitors.borrow().iter().enumerate().try_for_each(
            |(i, (id, monitor))| -> Result<(), Error> {
                write!(
                    writer,
                    "MONITOR {i}:\n\
                    ID: FQIN: {id}\n"
                )?;
                monitor.dump(writer, args)?;
                Result::Ok(())
            },
        )
    }
}

impl Drop for MonitorManager {
    fn drop(&mut self) {
        self.sb_recovery_events_processing.abort();
        self.hb_registration_commands_processing.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitor_model::test_utils::TestMonitor;
    use crate::tests::utils::StringWriter;

    use sdv_comms::id::ServiceFqin;
    use std::future;
    use std::str::FromStr;
    use tokio::sync::mpsc::channel;

    mod recovery_monitor_management_tests {
        use super::*;
        use crate::{
            hb_config::HeartbeatMonitoringConfig, hb_implicit_monitoring_db::InstanceConfig,
        };
        use std::time::Duration;
        use tokio::time::timeout;

        #[tokio::test]
        async fn test_create_implicit_hb_registration_command() {
            struct TestCase<'a> {
                input_incoming_event: ServiceBundleRecoveryEvent,
                input_current_state: Option<SBOrchestrationState>,
                input_config_db: &'a ImplicitMonitoringConfigRepo,
                assertion: Box<dyn FnOnce(Option<HBRegistrationCommand>) -> bool>,
                test_case_number: usize,
            }

            let fqin = ServiceFqin::builder()
                .sdv_vm_name("ignored")
                .sdv_package_name("com.google.test")
                .service_bundle_name("TestSB")
                .service_instance_name("instance1")
                .build()
                .unwrap();

            let config_db = ImplicitMonitoringConfigRepo::new_for_testing(HashMap::from([(
                fqin.clone(),
                InstanceConfig {
                    hb_config: Some(HeartbeatMonitoringConfig::default()),
                    qos_config: HashMap::new(),
                },
            )]));
            let empty_config_db = ImplicitMonitoringConfigRepo::new_for_testing(HashMap::default());

            let testcases = [
                /* Case 0 - A sequence of states that could lead to ImplicitCommand creation,
                   but service bundle is not registered for implicit monitoring
                */
                TestCase {
                    input_incoming_event: ServiceBundleRecoveryEvent {
                        fqin: fqin.clone(),
                        state: SBOrchestrationState {
                            recovery_state: SBRecoveryState::Normal,
                            lifecycle_state: SBLifecycleState::Started,
                        },
                    },
                    input_current_state: None,
                    input_config_db: &empty_config_db,
                    assertion: Box::new(|c| c.is_none()),
                    test_case_number: 0,
                },
                // /* Case 1 - A sequence of states that lead to ImplicitRegister creation,
                //    as service bundle is registered for implicit monitoring

                //    Note that although incoming event shows that the bundle malfunctioned/crashed,
                //    we still register a monitor.
                // */
                TestCase {
                    input_incoming_event: ServiceBundleRecoveryEvent {
                        fqin: fqin.clone(),
                        state: SBOrchestrationState {
                            recovery_state: SBRecoveryState::Recovering,
                            lifecycle_state: SBLifecycleState::Started,
                        },
                    },
                    input_current_state: Some(SBOrchestrationState {
                        recovery_state: SBRecoveryState::Normal,
                        lifecycle_state: SBLifecycleState::Created,
                    }),
                    input_config_db: &config_db,
                    assertion: Box::new(|c: Option<HBRegistrationCommand>| {
                        c.is_some_and(|c| {
                            matches!(c.action, HBRegistrationAction::RegisterImplicitly(_))
                        })
                    }),
                    test_case_number: 1,
                },
                /* Case 2 - A sequence of states that lead to ImplicitUnregister creation,
                   as service bundle is registered for implicit monitoring

                   Note that although previous state was recovering, as long as new state
                   is normal, we create an unregistration command
                */
                TestCase {
                    input_incoming_event: ServiceBundleRecoveryEvent {
                        fqin: fqin.clone(),
                        state: SBOrchestrationState {
                            recovery_state: SBRecoveryState::Normal,
                            lifecycle_state: SBLifecycleState::Created,
                        },
                    },
                    input_current_state: Some(SBOrchestrationState {
                        recovery_state: SBRecoveryState::Recovering,
                        lifecycle_state: SBLifecycleState::Started,
                    }),
                    input_config_db: &config_db,
                    assertion: Box::new(|c: Option<HBRegistrationCommand>| {
                        c.is_some_and(|c| {
                            matches!(c.action, HBRegistrationAction::UnregisterImplicitly)
                        })
                    }),
                    test_case_number: 2,
                },
                /* Case 3 - A sequence of states that does not lead ImplicitUnregister creation,
                   As new event shows that bundle is recovering
                */
                TestCase {
                    input_incoming_event: ServiceBundleRecoveryEvent {
                        fqin: fqin.clone(),
                        state: SBOrchestrationState {
                            recovery_state: SBRecoveryState::Recovering,
                            lifecycle_state: SBLifecycleState::Created,
                        },
                    },
                    input_current_state: Some(SBOrchestrationState {
                        recovery_state: SBRecoveryState::Normal,
                        lifecycle_state: SBLifecycleState::Started,
                    }),
                    input_config_db: &config_db,
                    assertion: Box::new(|c: Option<HBRegistrationCommand>| c.is_none()),
                    test_case_number: 3,
                },
                /* Case 4 - A sequence of states that does not lead ImplicitUnregister creation,
                   although bundle is configured for implicit monitoring
                */
                TestCase {
                    input_incoming_event: ServiceBundleRecoveryEvent {
                        fqin: fqin.clone(),
                        state: SBOrchestrationState {
                            recovery_state: SBRecoveryState::Normal,
                            lifecycle_state: SBLifecycleState::Started,
                        },
                    },
                    input_current_state: None,
                    input_config_db: &empty_config_db,
                    assertion: Box::new(|c| c.is_none()),
                    test_case_number: 4,
                },
                /* Case 5 - A sequence of states that should not leado ImplicitRegister creation:
                   Bundle is already in started state, command was created when first entered
                   started state.
                */
                TestCase {
                    input_incoming_event: ServiceBundleRecoveryEvent {
                        fqin: fqin.clone(),
                        state: SBOrchestrationState {
                            recovery_state: SBRecoveryState::Normal,
                            lifecycle_state: SBLifecycleState::Started,
                        },
                    },
                    input_current_state: Some(SBOrchestrationState {
                        recovery_state: SBRecoveryState::Recovering,
                        lifecycle_state: SBLifecycleState::Started,
                    }),
                    input_config_db: &config_db,
                    assertion: Box::new(|c| c.is_none()),
                    test_case_number: 5,
                },
            ];

            for TestCase {
                input_incoming_event,
                input_current_state,
                input_config_db,
                assertion,
                test_case_number,
            } in testcases
            {
                let result = MonitorManager::create_implicit_hb_registration_command(
                    input_incoming_event,
                    input_current_state,
                    input_config_db,
                )
                .await;
                assert!(
                    assertion(result),
                    "Testing creating implicit commands, test case {} failed",
                    test_case_number
                );
            }
        }

        #[tokio::test]
        async fn test_process_recovery_events() {
            let (event_tx, event_rx) = channel(10);
            let (monitor_tx, mut monitor_rx) = watch::channel(HashMap::new());
            let (hb_registration_commands_sink, _hb_registration_commands_rx) = channel(10);

            let processing_task = tokio::spawn(MonitorManager::process_recovery_events(
                ImplicitMonitoringConfigRepo::new_for_testing(HashMap::default()),
                event_rx,
                monitor_tx,
                hb_registration_commands_sink,
            ));

            let fqin1 = ServiceFqin::builder()
                .sdv_vm_name("ignored")
                .sdv_package_name("com.google.test")
                .service_bundle_name("TestSB")
                .service_instance_name("instance1")
                .build()
                .unwrap();
            let fqin2 = ServiceFqin::builder()
                .sdv_vm_name("ignored")
                .sdv_package_name("com.google.test")
                .service_bundle_name("TestSB")
                .service_instance_name("instance2")
                .build()
                .unwrap();

            // Initial event
            timeout(
                Duration::from_millis(200),
                event_tx.send(ServiceBundleRecoveryEvent {
                    fqin: fqin1.clone(),
                    state: SBOrchestrationState {
                        recovery_state: SBRecoveryState::Normal,
                        lifecycle_state: SBLifecycleState::Created,
                    },
                }),
            )
            .await
            .unwrap()
            .unwrap();

            // Check state
            timeout(Duration::from_millis(200), monitor_rx.changed()).await.unwrap().unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(monitors.get(&fqin1).unwrap().recovery_state, SBRecoveryState::Normal);
            }

            // Second event for the same fqin
            event_tx
                .send(ServiceBundleRecoveryEvent {
                    fqin: fqin1.clone(),
                    state: SBOrchestrationState {
                        recovery_state: SBRecoveryState::Recovering,
                        lifecycle_state: SBLifecycleState::Created,
                    },
                })
                .await
                .unwrap();

            timeout(Duration::from_millis(200), monitor_rx.changed()).await.unwrap().unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(
                    monitors.get(&fqin1).unwrap().recovery_state,
                    SBRecoveryState::Recovering
                );
            }

            // Event for a different fqin
            event_tx
                .send(ServiceBundleRecoveryEvent {
                    fqin: fqin2.clone(),
                    state: SBOrchestrationState {
                        recovery_state: SBRecoveryState::Normal,
                        lifecycle_state: SBLifecycleState::Created,
                    },
                })
                .await
                .unwrap();

            timeout(Duration::from_millis(200), monitor_rx.changed()).await.unwrap().unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(monitors.len(), 2);
                assert_eq!(monitors.get(&fqin2).unwrap().recovery_state, SBRecoveryState::Normal);
            }

            // A new, valid transition (SB started again),
            // proving that state machine is in a functional state
            event_tx
                .send(ServiceBundleRecoveryEvent {
                    fqin: fqin1.clone(),
                    state: SBOrchestrationState {
                        recovery_state: SBRecoveryState::Normal,
                        lifecycle_state: SBLifecycleState::Created,
                    },
                })
                .await
                .unwrap();

            timeout(Duration::from_millis(200), monitor_rx.changed()).await.unwrap().unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(monitors.get(&fqin1).unwrap().recovery_state, SBRecoveryState::Normal);
            }

            // A new event with same recovery state but different lifecycle state should update the
            // monitor
            event_tx
                .send(ServiceBundleRecoveryEvent {
                    fqin: fqin1.clone(),
                    state: SBOrchestrationState {
                        recovery_state: SBRecoveryState::Normal,
                        lifecycle_state: SBLifecycleState::Started,
                    },
                })
                .await
                .unwrap();

            timeout(Duration::from_millis(200), monitor_rx.changed()).await.unwrap().unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(monitors.get(&fqin1).unwrap().recovery_state, SBRecoveryState::Normal);
                assert_eq!(
                    monitors.get(&fqin1).unwrap().lifecycle_state,
                    SBLifecycleState::Started
                );
            }

            drop(event_tx);
            processing_task.await.unwrap();
        }
    }

    mod hb_monitor_management_tests {
        use super::*;
        use crate::hb_explicit_registration::HBRegistrationCommand;
        use crate::hb_monitor_factory::MockHeartbeatMonitorFactory;
        use crate::monitor_model::test_utils::TestMonitor;
        use futures::task::noop_waker_ref;
        use futures::task::Poll;
        use futures::Future;
        use std::task::Context;
        use std::time::Duration;
        use tokio::time::timeout;

        static HEALTHY_SERVICE_FQIN_STR: &str = "vm:com.google.sdv.HealthyServiceBundle/sample";

        #[tokio::test]
        async fn test_process_hb_registration_queue_explicit_registration_succeeds() {
            // SETUP
            let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();
            let monitor1 = TestMonitor::default();
            let config = monitor1.config;

            let mut monitor_factory = MockHeartbeatMonitorFactory::new();
            monitor_factory.expect_new_monitor().returning(move |_, _| Ok(Box::new(monitor1)));

            let (monitors_tx, monitors_rx) = tokio::sync::watch::channel(HashMap::new());
            let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);

            let (oneshot1_tx, oneshot1_rx) = tokio::sync::oneshot::channel();

            // ACT
            request_tx
                .send(HBRegistrationCommand {
                    fqin: fqin.clone(),
                    action: HBRegistrationAction::RegisterExplicitly(config, oneshot1_tx),
                })
                .await
                .unwrap();

            tokio::spawn(MonitorManager::process_hb_registration_queue(
                request_rx,
                monitors_tx,
                monitor_factory,
            ));

            // ASSERT
            assert!(oneshot1_rx.await.unwrap().is_ok());
            assert!(monitors_rx.borrow().contains_key(&fqin))
        }

        #[tokio::test]
        async fn test_process_hb_registration_queue_implicit_registration_succeeds() {
            // SETUP
            let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();
            let monitor1 = TestMonitor::default();
            let config = monitor1.config;

            let mut monitor_factory = MockHeartbeatMonitorFactory::new();
            monitor_factory.expect_new_monitor().returning(move |_, _| Ok(Box::new(monitor1)));

            let (monitors_tx, mut monitors_rx) = tokio::sync::watch::channel(HashMap::new());
            let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);

            // ACT
            request_tx
                .send(HBRegistrationCommand {
                    fqin: fqin.clone(),
                    action: HBRegistrationAction::RegisterImplicitly(config),
                })
                .await
                .unwrap();

            tokio::spawn(MonitorManager::process_hb_registration_queue(
                request_rx,
                monitors_tx,
                monitor_factory,
            ));

            // ASSERT
            timeout(Duration::from_secs(1), monitors_rx.wait_for(|map| map.contains_key(&fqin)))
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test]
        async fn test_process_hb_registration_queue_explicit_unregistration_succeeds() {
            // Tests that unregistration command correctly removes previously registered monitor
            // from internal monitor holding structure

            // SETUP
            let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();
            let monitor1 = Box::new(TestMonitor::default()) as Box<dyn EntityMonitor>;

            let (monitors_tx, monitors_rx) =
                tokio::sync::watch::channel(HashMap::from([(fqin.clone(), monitor1)]));
            let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);

            // ACT
            let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
            request_tx
                .send(HBRegistrationCommand {
                    fqin,
                    action: HBRegistrationAction::UnregisterExplicitly(oneshot_tx),
                })
                .await
                .unwrap();

            let monitor_factory = MockHeartbeatMonitorFactory::new();
            tokio::spawn(MonitorManager::process_hb_registration_queue(
                request_rx,
                monitors_tx,
                monitor_factory,
            ));

            // ASSERT
            oneshot_rx.await.unwrap().unwrap();
            assert!(monitors_rx.borrow().is_empty());
        }

        #[tokio::test]
        async fn test_process_hb_registration_queue_implicit_unregistration_succeeds() {
            // Tests that implicit unregistration command correctly removes previously registered
            // monitor from internal monitor holding structure

            // SETUP
            let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();
            let monitor1 = Box::new(TestMonitor::default()) as Box<dyn EntityMonitor>;

            let (monitors_tx, mut monitors_rx) =
                tokio::sync::watch::channel(HashMap::from([(fqin.clone(), monitor1)]));
            let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);

            // ACT
            request_tx
                .send(HBRegistrationCommand {
                    fqin,
                    action: HBRegistrationAction::UnregisterImplicitly,
                })
                .await
                .unwrap();

            let monitor_factory = MockHeartbeatMonitorFactory::new();
            tokio::spawn(MonitorManager::process_hb_registration_queue(
                request_rx,
                monitors_tx,
                monitor_factory,
            ));

            // ASSERT
            timeout(Duration::from_secs(1), monitors_rx.wait_for(|map| map.is_empty()))
                .await
                .unwrap()
                .unwrap();
        }

        #[tokio::test]
        async fn test_process_hb_registration_queue_re_registration_post_always_succeeds() {
            // test setup: 2 registration requests: same fqin, same config ->
            // Until a direct way of determining whether process has crashed is implemented, HM
            // serves re-registration commands without returning error, regardless of monitor
            // existing
            let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();

            let monitor1 = TestMonitor::default();
            let config = monitor1.config;

            let mut monitor_factory = MockHeartbeatMonitorFactory::new();
            monitor_factory.expect_new_monitor().returning(move |_, _| Ok(Box::new(monitor1)));

            let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);
            let (oneshot1_tx, oneshot1_rx) = tokio::sync::oneshot::channel();
            let (oneshot2_tx, oneshot2_rx) = tokio::sync::oneshot::channel();

            request_tx
                .send(HBRegistrationCommand {
                    fqin: fqin.clone(),
                    action: HBRegistrationAction::RegisterExplicitly(config, oneshot1_tx),
                })
                .await
                .unwrap();
            request_tx
                .send(HBRegistrationCommand {
                    fqin,
                    action: HBRegistrationAction::RegisterExplicitly(config, oneshot2_tx),
                })
                .await
                .unwrap();

            let (monitors_tx, _monitors_rx) = tokio::sync::watch::channel(HashMap::new());
            tokio::spawn(MonitorManager::process_hb_registration_queue(
                request_rx,
                monitors_tx,
                monitor_factory,
            ));

            assert!(oneshot1_rx.await.unwrap().is_ok());
            assert!(oneshot2_rx.await.unwrap().is_ok());
        }

        #[tokio::test]
        async fn test_rpc_hb_registration_disconnect_does_not_lead_to_panic_b439620660() {
            // setup
            let (register_commands_tx, register_commands_rx) = tokio::sync::mpsc::channel(10);
            let healthy_service_monitor = TestMonitor::default();
            let config = healthy_service_monitor.config;

            let (registration_result_tx, registration_result_rx) = tokio::sync::oneshot::channel();
            register_commands_tx
                .send(HBRegistrationCommand {
                    fqin: ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
                    action: HBRegistrationAction::RegisterExplicitly(
                        config,
                        registration_result_tx,
                    ),
                })
                .await
                .unwrap();

            //setup for running the future once
            let (monitors_tx, _monitors_rx) = tokio::sync::watch::channel(HashMap::new());
            let mut monitor_factory = MockHeartbeatMonitorFactory::new();
            monitor_factory
                .expect_new_monitor()
                .returning(move |_, _| Ok(Box::new(healthy_service_monitor)));

            let mut pinned_fut = Box::pin(MonitorManager::process_hb_registration_queue(
                register_commands_rx,
                monitors_tx,
                monitor_factory,
            ));
            let mut cx = Context::from_waker(noop_waker_ref());

            // simulate registration cancelled early:
            std::mem::drop(registration_result_rx);

            // action: manual poll,
            (1..10).for_each(|_| match pinned_fut.as_mut().poll(&mut cx) {
                Poll::Ready(_) => {
                    panic!("this future should not be ready under normal circumstances")
                }
                Poll::Pending => {}
            });
            // if test made it here without panicking, behaviour is correct
        }
    }

    #[tokio::test]
    async fn test_monitor_manager_dumpsys() {
        let monitored_sb_fqin = "vm:com.google.test.TestSB/instance1";

        let agent_crash_monitors = HashMap::from([(
            "Hello Agent".to_string(),
            Box::new(TestMonitor::default()) as Box<dyn EntityMonitor>,
        )]);

        let (_, hb_monitors) = watch::channel(HashMap::from([(
            ServiceFqin::from_str(monitored_sb_fqin).unwrap(),
            Box::new(TestMonitor::default()) as Box<dyn EntityMonitor>,
        )]));
        let (_, sb_recovery_monitors) = watch::channel(HashMap::from([(
            ServiceFqin::from_str(monitored_sb_fqin).unwrap(),
            SBOrchestrationState {
                recovery_state: SBRecoveryState::Normal,
                lifecycle_state: SBLifecycleState::Created,
            },
        )]));
        let mm = MonitorManager {
            sb_recovery_events_processing: tokio::spawn(future::pending()),
            hb_registration_commands_processing: tokio::spawn(future::pending()),
            sb_recovery_monitors,
            agent_crash_monitors,
            hb_monitors,
        };

        let mut writer = StringWriter::new();
        mm.dump(&mut writer, &[]).unwrap();

        assert!(
            writer.sink == "\nHEARTBEAT MONITORING:\n\
                MONITOR 0:\n\
                ID: FQIN: vm:com.google.test.TestSB/instance1\n\
                TEST MONITOR\n\
                Healthy: Healthy\n\
                Config: HeartbeatMonitoringConfig { initial_delay_ms: 0, period_ms: 0, num_periods: 0, task_duration_ms: 0 }\n\
                    \n\
                RECOVERY MONITORING:\n\
                a. Agent monitoring:\n\
                MONITOR 0:\n\
                ID: Agent: Hello Agent\n\
                TEST MONITOR\n\
                Healthy: Healthy\n\
                Config: HeartbeatMonitoringConfig { initial_delay_ms: 0, period_ms: 0, num_periods: 0, task_duration_ms: 0 }\n\
                b. SB monitoring:\n\
                MONITOR 0:\n\
                ID: FQIN: vm:com.google.test.TestSB/instance1\n\
                Recovery State: Normal\n\
                Lifecycle State: Created\n\
                Health Status: Healthy\n",
            "Dumpsys is not what we expect. Here it is:{}",
            writer.sink
        );
    }
}
