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
use sdv_dump::DumpStateReporter;
use std::collections::HashMap;
use std::io::Error;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::monitor_model::EntityMonitor;
use crate::monitor_model::SBRecoveryState;
use crate::{monitor_model::MonitorId, sb_recovery_listener::ServiceBundleRecoveryEvent};

//TODO: add HBMonitors to struct
pub struct MonitorManager {
    sb_recovery_events_processing: JoinHandle<()>,
    pub sb_recovery_monitors: watch::Receiver<HashMap<MonitorId, SBRecoveryState>>,
    //TODO: enhance agent crash monitoring with recovery monitoring capabilities
    pub agent_crash_monitors: HashMap<MonitorId, Box<dyn EntityMonitor>>,
}
impl MonitorManager {
    pub fn new(
        sb_recovery_events: mpsc::Receiver<ServiceBundleRecoveryEvent>,
        agent_crash_monitors: HashMap<MonitorId, Box<dyn EntityMonitor>>,
    ) -> Self {
        let (tx, sb_recovery_monitors) = watch::channel(HashMap::new());
        let sb_recovery_events_processing =
            tokio::spawn(Self::process_recovery_events(sb_recovery_events, tx));
        Self { sb_recovery_events_processing, sb_recovery_monitors, agent_crash_monitors }
    }

    async fn process_recovery_events(
        mut incoming_events: mpsc::Receiver<ServiceBundleRecoveryEvent>,
        monitor_sink: watch::Sender<HashMap<MonitorId, SBRecoveryState>>,
    ) {
        while let Some(event) = incoming_events.recv().await {
            monitor_sink.send_if_modified(|monitors| {
                let id = MonitorId::BundleRecoveryMonitorId(Box::new(event.fqin));
                let current = monitors.get(&id);
                let res = check_sb_recovery_transition(current, &event.new_state);
                if res.is_err() {
                    error!("For {id}: {res:?}. Yet, accepting new state");
                }

                monitors.insert(id, event.new_state);
                true
            });
        }
    }
}
impl DumpStateReporter for MonitorManager {
    fn dump(&self, writer: &mut dyn std::io::Write, args: &[&std::ffi::CStr]) -> Result<(), Error> {
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
                    ID: {id}\n"
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
                    ID: {id}\n"
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
    }
}

fn check_sb_recovery_transition(
    current: Option<&SBRecoveryState>,
    new: &SBRecoveryState,
) -> anyhow::Result<()> {
    match (current, new) {
        // illegal, transition to same state:
        (Some(state1),state2) if state1 == state2 =>
            Err(anyhow!("Illegal, same transition: {current:?} -> {new:?}")),
        // illegal. From recovering, SB either fails recovery, or turns back to normal
        (Some(SBRecoveryState::Recovering),SBRecoveryState::TerminatedClean) |
        // illegal, or at least strange, that first state received from orch is terminal:
        (None, SBRecoveryState::FailedRecovery) |
        (None, SBRecoveryState::TerminatedClean) =>
            Err(anyhow!("Illegal, same transition: {current:?} -> {new:?}")),
        //normal
        _ => Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::utils::StringWriter;

    use super::*;
    use sdv_comms::id::ServiceFqin;
    use std::future;
    use std::str::FromStr;
    use tokio::sync::mpsc::channel;

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
            assert_eq!(
                monitors.get(&MonitorId::BundleRecoveryMonitorId(Box::new(fqin1.clone()))),
                Some(&SBRecoveryState::Normal)
            );
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
            assert_eq!(
                monitors.get(&MonitorId::BundleRecoveryMonitorId(Box::new(fqin1.clone()))),
                Some(&SBRecoveryState::Recovering)
            );
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
            assert_eq!(
                monitors.get(&MonitorId::BundleRecoveryMonitorId(Box::new(fqin2.clone()))),
                Some(&SBRecoveryState::Normal)
            );
        }

        // Illegal transition (recovering -> terminated_clean) for fqin1
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
            assert_eq!(
                monitors.get(&MonitorId::BundleRecoveryMonitorId(Box::new(fqin1.clone()))),
                Some(&SBRecoveryState::TerminatedClean)
            );
        }

        // A new, legal transition(terminated_clean->normal) (SB started again),
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
            assert_eq!(
                monitors.get(&MonitorId::BundleRecoveryMonitorId(Box::new(fqin1.clone()))),
                Some(&SBRecoveryState::Normal)
            );
        }

        drop(event_tx);
        processing_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_monitor_manager_dumpsys() {
        let monitored_sb_fqin = "vm:com.google.test.TestSB/instance1";

        let agent_crash_monitors = HashMap::new();
        let (_, sb_recovery_monitors) = watch::channel(HashMap::from([(
            MonitorId::BundleRecoveryMonitorId(Box::new(
                ServiceFqin::from_str(monitored_sb_fqin).unwrap(),
            )),
            SBRecoveryState::Normal,
        )]));
        let mm = MonitorManager {
            sb_recovery_events_processing: tokio::spawn(future::pending()),
            sb_recovery_monitors,
            agent_crash_monitors,
        };

        let mut writer = StringWriter::new();
        mm.dump(&mut writer, &[]).unwrap();

        assert_eq!(
            writer.sink,
            "\nRECOVERY MONITORING:\n\
            a. Agent monitoring:\n\
            NO ACTIVE MONITORS\n\
            b. SB monitoring:\n\
            MONITOR 0:\n\
            ID: FQIN: vm:com.google.test.TestSB/instance1\n\
            Normal\n"
        );
    }
}
