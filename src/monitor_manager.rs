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

use anyhow::anyhow;
use log::error;
use log::warn;
use sdv_comms::id::ServiceFqin;
use sdv_dump::DumpStateReporter;
use sdv_status_rs::SdvResult;
use std::collections::HashMap;
use std::io::Error;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::hb_explicit_registration::HBRegistrationCommand;
use crate::hb_explicit_registration::RegistrationCommandError;
use crate::hb_monitor_factory::HeartbeatMonitorFactory;
use crate::monitor_model::AgentName;
use crate::monitor_model::EntityMonitor;
use crate::monitor_model::SBRecoveryState;
use crate::sb_recovery_listener::ServiceBundleRecoveryEvent;

pub struct MonitorManager {
    sb_recovery_events_processing: JoinHandle<()>,
    hb_registration_commands_processing: JoinHandle<()>,
    pub sb_recovery_monitors: watch::Receiver<HashMap<ServiceFqin, SBRecoveryState>>,
    //TODO: enhance agent crash monitoring with recovery monitoring capabilities
    pub agent_crash_monitors: HashMap<AgentName, Box<dyn EntityMonitor>>,
    pub hb_monitors: watch::Receiver<HashMap<ServiceFqin, Box<dyn EntityMonitor>>>,
}
impl MonitorManager {
    pub fn new(
        hb_registration_commands: mpsc::Receiver<HBRegistrationCommand>,
        sb_recovery_events: mpsc::Receiver<ServiceBundleRecoveryEvent>,
        agent_crash_monitors: HashMap<AgentName, Box<dyn EntityMonitor>>,
        hb_monitor_factory: impl HeartbeatMonitorFactory + 'static,
    ) -> Self {
        let (sb_recovery_tx, sb_recovery_monitors) = watch::channel(HashMap::new());
        let sb_recovery_events_processing =
            tokio::spawn(Self::process_recovery_events(sb_recovery_events, sb_recovery_tx));

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

    async fn process_recovery_events(
        mut incoming_events: mpsc::Receiver<ServiceBundleRecoveryEvent>,
        monitor_sink: watch::Sender<HashMap<ServiceFqin, SBRecoveryState>>,
    ) {
        while let Some(event) = incoming_events.recv().await {
            monitor_sink.send_if_modified(|monitors| {
                let current = monitors.get(&event.fqin);
                let res = check_sb_recovery_transition(current, &event.new_state);
                if res.is_err() {
                    error!("For {}: {:?}. Yet, accepting new state", event.fqin, res);
                }

                monitors.insert(event.fqin, event.new_state);
                true
            });
        }
    }

    async fn process_hb_registration_queue(
        mut registration_requests: mpsc::Receiver<HBRegistrationCommand>,
        monitors: watch::Sender<HashMap<ServiceFqin, Box<dyn EntityMonitor>>>,
        monitor_factory: impl HeartbeatMonitorFactory,
    ) {
        // TODO(tmot): cleanup logic, enhance logic, to take recovery state into account,
        // TODO(tmot): post move to MonitorManager
        while let Some(request) = registration_requests.recv().await {
            match request {
                HBRegistrationCommand::Register(fqin, config, response_tx) => {
                    let insert_monitor =
                        if let Some(existing_monitor) = monitors.borrow().get(&fqin) {
                            // insert if monitor found has same config: assuming instance has crashed
                            *existing_monitor.get_config() == config
                        } else {
                            true
                        };
                    if insert_monitor {
                        let new_monitor: SdvResult<Box<dyn EntityMonitor>> =
                            monitor_factory.new_monitor(&fqin, config).await;
                        let res = if let Ok(monitor) = new_monitor {
                            monitors.send_modify(|m| {
                                m.insert(fqin, monitor);
                            });
                            Ok(())
                        } else {
                            //TODO use SDVResult
                            Err(RegistrationCommandError::PublicationNotFound)
                        };
                        response_tx
                            .send(res)
                            .unwrap_or_else(|_| warn!("Oneshot receiver unexpectedly dropped!"));
                    } else {
                        response_tx
                            .send(Err(RegistrationCommandError::AlreadyRegistered))
                            .unwrap_or_else(|_| warn!("Oneshot receiver unexpectedly dropped!"));
                    }
                }
                HBRegistrationCommand::Unregister(fqin, response_tx) => {
                    monitors.send_modify(|m| {
                        m.remove(&fqin);
                    });
                    response_tx.send(()).unwrap_or_else(|_| {
                        warn!("Unregistration, oneshot receiver unexpectedly dropped!")
                    });
                }
            };
        }
        error!("HB Registration channel unexpectedly closed!");
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

fn check_sb_recovery_transition(
    current: Option<&SBRecoveryState>,
    new: &SBRecoveryState,
) -> anyhow::Result<()> {
    match (current, new) {
        // invalid, transition to same state:
        (Some(state1),state2) if state1 == state2 =>
            Err(anyhow!("Invalid, same transition: {current:?} -> {new:?}")),
        // invalid. From recovering, SB either fails recovery, or turns back to normal
        (Some(SBRecoveryState::Recovering),SBRecoveryState::TerminatedClean) |
        // invalid, or at least strange, that first state received from orch is terminal:
        (None, SBRecoveryState::FailedRecovery) |
        (None, SBRecoveryState::TerminatedClean) =>
            Err(anyhow!("Invalid, same transition: {current:?} -> {new:?}")),
        //normal
        _ => Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::monitor_model::test_utils::TestMonitor;
    use crate::tests::utils::StringWriter;

    use super::*;
    use sdv_comms::id::ServiceFqin;
    use std::future;
    use std::str::FromStr;
    use tokio::sync::mpsc::channel;

    mod recovery_monitor_management_tests {
        use super::*;

        #[test]
        fn test_check_sb_recovery_transition_valid() {
            let transitions = vec![
                (None, SBRecoveryState::Normal),
                (None, SBRecoveryState::Recovering),
                (Some(SBRecoveryState::Normal), SBRecoveryState::Recovering),
                (Some(SBRecoveryState::Recovering), SBRecoveryState::FailedRecovery),
                (Some(SBRecoveryState::Recovering), SBRecoveryState::Normal),
                (Some(SBRecoveryState::FailedRecovery), SBRecoveryState::Recovering),
            ];
            for (current, new) in transitions {
                assert!(check_sb_recovery_transition(current.as_ref(), &new).is_ok());
            }
        }

        #[test]
        fn test_check_sb_recovery_transition_invalid() {
            let transitions = vec![
                (Some(SBRecoveryState::Normal), SBRecoveryState::Normal),
                (Some(SBRecoveryState::Recovering), SBRecoveryState::Recovering),
                (Some(SBRecoveryState::FailedRecovery), SBRecoveryState::FailedRecovery),
                (Some(SBRecoveryState::TerminatedClean), SBRecoveryState::TerminatedClean),
                (Some(SBRecoveryState::Recovering), SBRecoveryState::TerminatedClean),
                (None, SBRecoveryState::FailedRecovery),
                (None, SBRecoveryState::TerminatedClean),
            ];
            for (current, new) in transitions {
                assert!(check_sb_recovery_transition(current.as_ref(), &new).is_err());
            }
        }

        #[tokio::test]
        async fn test_process_recovery_events() {
            let (event_tx, event_rx) = channel(10);
            let (monitor_tx, mut monitor_rx) = watch::channel(HashMap::new());

            let processing_task =
                tokio::spawn(MonitorManager::process_recovery_events(event_rx, monitor_tx));

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
            event_tx
                .send(ServiceBundleRecoveryEvent {
                    fqin: fqin1.clone(),
                    new_state: SBRecoveryState::Normal,
                })
                .await
                .unwrap();

            // Check state
            monitor_rx.changed().await.unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(monitors.get(&fqin1), Some(&SBRecoveryState::Normal));
            }

            // Second event for the same fqin
            event_tx
                .send(ServiceBundleRecoveryEvent {
                    fqin: fqin1.clone(),
                    new_state: SBRecoveryState::Recovering,
                })
                .await
                .unwrap();

            monitor_rx.changed().await.unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(monitors.get(&fqin1), Some(&SBRecoveryState::Recovering));
            }

            // Event for a different fqin
            event_tx
                .send(ServiceBundleRecoveryEvent {
                    fqin: fqin2.clone(),
                    new_state: SBRecoveryState::Normal,
                })
                .await
                .unwrap();

            monitor_rx.changed().await.unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(monitors.len(), 2);
                assert_eq!(monitors.get(&fqin2), Some(&SBRecoveryState::Normal));
            }

            // Invalid transition (recovering -> terminated_clean) for fqin1
            // change should STILL happen (we just log the error)
            event_tx
                .send(ServiceBundleRecoveryEvent {
                    fqin: fqin1.clone(),
                    new_state: SBRecoveryState::TerminatedClean,
                })
                .await
                .unwrap();
            monitor_rx.changed().await.unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(monitors.get(&fqin1), Some(&SBRecoveryState::TerminatedClean));
            }

            // A new, valid transition(terminated_clean->normal) (SB started again),
            // proving that state machine is in a functional state
            event_tx
                .send(ServiceBundleRecoveryEvent {
                    fqin: fqin1.clone(),
                    new_state: SBRecoveryState::Normal,
                })
                .await
                .unwrap();
            monitor_rx.changed().await.unwrap();
            {
                let monitors = monitor_rx.borrow_and_update();
                assert_eq!(monitors.get(&fqin1), Some(&SBRecoveryState::Normal));
            }

            drop(event_tx);
            processing_task.await.unwrap();
        }
    }

    mod hb_monitor_management_tests {
        use super::*;
        use crate::hb_config::HeartbeatMonitoringConfig;
        use crate::hb_explicit_registration::HBRegistrationCommand;
        use crate::hb_monitor_factory::MockHeartbeatMonitorFactory;
        use crate::monitor_model::test_utils::TestMonitor;
        use futures::task::noop_waker_ref;
        use futures::task::Poll;
        use futures::Future;
        use std::task::Context;

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
                .send(HBRegistrationCommand::Register(fqin.clone(), config, oneshot1_tx))
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
        async fn test_process_hb_registration_queue_explicit_unregistration_succeeds() {
            // Tests that unregsitration command correctly removes previously registered monitor
            // from internal monitor holding structure

            // SETUP
            let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();
            let monitor1 = Box::new(TestMonitor::default()) as Box<dyn EntityMonitor>;

            let (monitors_tx, monitors_rx) =
                tokio::sync::watch::channel(HashMap::from([(fqin.clone(), monitor1)]));
            let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);

            // ACT
            let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
            request_tx.send(HBRegistrationCommand::Unregister(fqin, oneshot_tx)).await.unwrap();

            let monitor_factory = MockHeartbeatMonitorFactory::new();
            tokio::spawn(MonitorManager::process_hb_registration_queue(
                request_rx,
                monitors_tx,
                monitor_factory,
            ));

            // ASSERT
            oneshot_rx.await.unwrap();
            assert!(monitors_rx.borrow().is_empty());
        }

        #[tokio::test]
        async fn test_process_hb_registration_queue_double_register_command_returns_fails() {
            // test setup: 2 registration requests: same fqin, but different configs ->
            // the current logic considers this a case of double registration (based on the difference
            // in configs)
            let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();

            let monitor1 = TestMonitor::default();
            let config1 = monitor1.config;

            let monitor2 = TestMonitor {
                config: HeartbeatMonitoringConfig {
                    initial_delay_ms: 0xCAFEu64,
                    ..Default::default()
                },
                ..Default::default()
            };
            let config2 = monitor2.config;

            let mut monitor_factory = MockHeartbeatMonitorFactory::new();
            monitor_factory.expect_new_monitor().returning(move |_, _| Ok(Box::new(monitor1)));

            let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);
            let (oneshot1_tx, oneshot1_rx) = tokio::sync::oneshot::channel();
            let (oneshot2_tx, oneshot2_rx) = tokio::sync::oneshot::channel();

            request_tx
                .send(HBRegistrationCommand::Register(fqin.clone(), config1, oneshot1_tx))
                .await
                .unwrap();
            request_tx
                .send(HBRegistrationCommand::Register(fqin, config2, oneshot2_tx))
                .await
                .unwrap();

            let (monitors_tx, _monitors_rx) = tokio::sync::watch::channel(HashMap::new());
            tokio::spawn(MonitorManager::process_hb_registration_queue(
                request_rx,
                monitors_tx,
                monitor_factory,
            ));

            assert!(oneshot1_rx.await.unwrap().is_ok());
            assert!(oneshot2_rx.await.unwrap().is_err());
        }

        #[tokio::test]
        async fn test_process_hb_registration_queue_re_registration_post_bundle_crash_succeeds() {
            // test setup: 2 registration requests: same fqin, same config ->
            // the current logic considers this a case of valid reregistration, as bundle might have crashed
            let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();

            let monitor1 = TestMonitor::default();
            let config = monitor1.config;

            let mut monitor_factory = MockHeartbeatMonitorFactory::new();
            monitor_factory.expect_new_monitor().returning(move |_, _| Ok(Box::new(monitor1)));

            let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);
            let (oneshot1_tx, oneshot1_rx) = tokio::sync::oneshot::channel();
            let (oneshot2_tx, oneshot2_rx) = tokio::sync::oneshot::channel();

            request_tx
                .send(HBRegistrationCommand::Register(fqin.clone(), config, oneshot1_tx))
                .await
                .unwrap();
            request_tx
                .send(HBRegistrationCommand::Register(fqin, config, oneshot2_tx))
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
                .send(HBRegistrationCommand::Register(
                    ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
                    config,
                    registration_result_tx,
                ))
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
            match pinned_fut.as_mut().poll(&mut cx) {
                Poll::Ready(_) => {
                    panic!("this future should not be ready under normal circumstances")
                }
                Poll::Pending => {}
            }
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
            SBRecoveryState::Normal,
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
                Normal\n",
            "Dumpsys is not what we expect. Here it is:{}",
            writer.sink
        );
    }
}
