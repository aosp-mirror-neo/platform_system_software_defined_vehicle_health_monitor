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
#![allow(missing_docs)]

pub use crate::health_reporter::MessagePublisher;
use crate::monitor_manager::MonitorManager;
use crate::monitor_model::AgentName;
use crate::monitor_model::EntityHealthStatus;
use crate::monitor_model::EntityMonitor;
use crate::sb_recovery_monitor::{SBLifecycleState, SBRecoveryMonitor, SBRecoveryState};
use async_trait::async_trait;
use authorization::AuthorizationImpl;
pub use com_android_sdv_health_health_monitor_service_rpc::Interface as HealthMonitorService;
use health_api::{
    health_monitor_service::{
        ListAllServiceBundlesRequest, ListAllServiceBundlesResponse,
        ListCrashingServiceBundlesRequest, ListCrashingServiceBundlesResponse,
        ListUnhealthyMonitoredServiceBundlesRequest, ListUnhealthyMonitoredServiceBundlesResponse,
    },
    health_topic::VmHealth,
};
use health_reporter::HealthReporter;
use log::{error, warn};
use protobuf::well_known_types::timestamp::Timestamp;
use sdv_comms::id::ServiceFqin;
pub use sdv_dump::DumpBinderConnection;
use sdv_dump::DumpStateReporter;
use std::sync::Mutex;
use std::time::Duration;
use std::{
    collections::HashMap,
    io::Error,
    ops::Add,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Instant,
};
use tracing::instrument;

pub mod agent_monitor;
pub mod authorization;
pub mod commstack_trait;
mod hb_config;
pub mod hb_explicit_registration;
pub mod hb_implicit_monitoring_db;
pub mod hb_monitor;
pub mod hb_monitor_factory;
mod hb_stream_ops;
mod health_reporter;
pub mod monitor_manager;
pub mod monitor_model;
pub mod power_state;
mod qos_config;
mod qos_monitor;
pub mod qos_registration_listener;
pub mod read_sys_prop;
pub mod sb_recovery_listener;
mod sb_recovery_monitor;
mod utils;
pub mod vm_config;

pub struct HealthMonitor {
    background_thread: Mutex<Option<thread::JoinHandle<()>>>,
    should_run: Arc<AtomicBool>,
    monitor_manager: Arc<MonitorManager>,
}

impl HealthMonitor {
    pub fn new(monitor_manager: Arc<MonitorManager>) -> Self {
        Self {
            background_thread: Mutex::new(None),
            should_run: Arc::new(AtomicBool::new(false)),
            monitor_manager,
        }
    }

    #[instrument(skip(self, publisher))]
    pub fn start(&self, publisher: Box<dyn MessagePublisher<VmHealth>>, report_period_ms: u64) {
        self.should_run.store(true, Ordering::SeqCst);

        let should_run = self.should_run.clone();
        let monitor_manager = Arc::clone(&self.monitor_manager);

        *self.background_thread.lock().unwrap() = Some(thread::spawn(move || {
            let health_reporter = HealthReporter::new(publisher);

            while should_run.load(Ordering::SeqCst) {
                let next_report_time = Instant::now().add(Duration::from_millis(report_period_ms));

                // Report overall VM health
                Self::report_state(
                    &monitor_manager.hb_monitors.borrow(),
                    &monitor_manager.sb_recovery_monitors.borrow(),
                    &monitor_manager.agent_crash_monitors,
                    &health_reporter,
                );

                // Sleep until next reporting period
                let sleep_period = next_report_time.saturating_duration_since(Instant::now());
                if sleep_period == Duration::ZERO {
                    error!("HM Agent VM health report takes too long. Check system configuration");
                }
                thread::sleep(sleep_period);
            }
        }));
    }

    fn report_state(
        heartbeat_monitors: &HashMap<ServiceFqin, Box<dyn EntityMonitor>>,
        sb_recovery_state_monitors: &HashMap<ServiceFqin, SBRecoveryMonitor>,
        agent_crash_monitors: &HashMap<AgentName, Box<dyn EntityMonitor>>,
        health_reporter: &HealthReporter,
    ) {
        let hbs_missed = heartbeat_monitors
            .values()
            .any(|m| m.get_health_status() == EntityHealthStatus::Unhealthy);

        let agent_recoveries_failed = agent_crash_monitors
            .values()
            .any(|m| m.get_health_status() == EntityHealthStatus::Unhealthy);
        let sb_recoveries_failed = sb_recovery_state_monitors
            .values()
            .any(|m| m.get_health_status() == EntityHealthStatus::Unhealthy);

        let recoveries_failed = agent_recoveries_failed || sb_recoveries_failed;
        if let Err(err) = health_reporter.report_vm_health(hbs_missed, recoveries_failed) {
            error!("Failed to report vm health: {err}");
        }
    }

    #[instrument(skip(self))]
    fn stop(&mut self) {
        self.should_run.store(false, Ordering::Relaxed);
        if let Some(thread) = self.background_thread.lock().unwrap().take() {
            thread.join().unwrap();
        }
    }
}

impl Drop for HealthMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

#[async_trait]
#[allow(non_snake_case)]
impl HealthMonitorService for HealthMonitor {
    #[instrument(skip(self))]
    async fn ListAllServiceBundles(
        &self,
        _fqin: sdv_comms::id::ServiceFqin,
        _request: &ListAllServiceBundlesRequest,
    ) -> sdv_status_rs::SdvResult<ListAllServiceBundlesResponse> {
        let running_bundles = self
            .monitor_manager
            .sb_recovery_monitors
            .borrow()
            .iter()
            .filter(|(_, orch_state)| {
                orch_state.recovery_state == SBRecoveryState::Normal
                    && (orch_state.lifecycle_state == SBLifecycleState::Created
                        || orch_state.lifecycle_state == SBLifecycleState::Started)
            })
            .map(|(fqin, _)| fqin.to_string())
            .collect();

        Ok(ListAllServiceBundlesResponse { fqin: running_bundles, ..Default::default() })
    }

    #[instrument(skip(self))]
    async fn ListCrashingServiceBundles(
        &self,
        _fqin: sdv_comms::id::ServiceFqin,
        _request: &ListCrashingServiceBundlesRequest,
    ) -> sdv_status_rs::SdvResult<ListCrashingServiceBundlesResponse> {
        let crashing_bundles = self
            .monitor_manager
            .sb_recovery_monitors
            .borrow()
            .iter()
            .filter(|(_, orch_state)| orch_state.recovery_state == SBRecoveryState::FailedRecovery)
            .map(|(fqin, _)| fqin.to_string())
            .collect();

        Ok(ListCrashingServiceBundlesResponse { fqin: crashing_bundles, ..Default::default() })
    }

    #[instrument(skip(self))]
    async fn ListUnhealthyMonitoredServiceBundles(
        &self,
        _fqin: sdv_comms::id::ServiceFqin,
        _request: &ListUnhealthyMonitoredServiceBundlesRequest,
    ) -> sdv_status_rs::SdvResult<ListUnhealthyMonitoredServiceBundlesResponse> {
        let unhealthy_services: Vec<String> = self
            .monitor_manager
            .hb_monitors
            .borrow()
            .iter()
            .filter(|(_, value)| value.get_health_status() == EntityHealthStatus::Unhealthy)
            .map(|(fqin, _)| fqin.to_string())
            .collect();

        Ok(ListUnhealthyMonitoredServiceBundlesResponse {
            fqin: unhealthy_services,
            ..Default::default()
        })
    }
}

impl DumpStateReporter for HealthMonitor {
    fn dump(
        &self,
        writer: &mut dyn std::io::Write,
        _args: &[&std::ffi::CStr],
    ) -> Result<(), Error> {
        let nanos_from_epoch_now = Timestamp::now();
        let nanos_from_epoch_now =
            Duration::new(nanos_from_epoch_now.seconds as u64, nanos_from_epoch_now.nanos as u32)
                .as_nanos();
        write!(
            writer,
            "CURR TIMESTAMP(ns): {}\n\
            INTERNAL STATE:\n\
            background_thread running: {}\n\
            should_run: {:?}\n\n",
            nanos_from_epoch_now,
            self.background_thread.lock().unwrap().is_some(),
            self.should_run
        )
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::hb_implicit_monitoring_db::ImplicitMonitoringConfigRepo;
    use crate::hb_monitor_factory::MockHeartbeatMonitorFactory;
    use crate::health_reporter::MockMessagePublisher;
    use crate::monitor_model::test_utils::TestMonitor;
    use crate::sb_recovery_monitor::{SBLifecycleState, SBOrchestrationState, SBRecoveryState};
    use crate::tests::utils::StringWriter;
    use mockall::predicate::eq;
    use sdv_comms::id::ServiceFqin;
    use std::str::FromStr;
    use tokio::sync::mpsc;
    use tokio::sync::watch;

    static HEALTHY_SERVICE_FQIN_STR: &str = "vm:com.google.sdv.HealthyServiceBundle/sample";
    static UNHEALTHY_SERVICE_FQIN_STR: &str = "vm:com.google.sdv.UnhealthyServiceBundle/sample";

    #[tokio::test]
    async fn test_start_starts_background_thread_and_can_be_stopped() {
        // SETUP
        let hm = {
            let (hb_registration_commands_sink, hb_registration_commands) = mpsc::channel(1);
            let (_, hb_recovery_events) = mpsc::channel(1);
            let (_, qos_events) = mpsc::channel(1);
            let monitor_manager = MonitorManager::new(
                ImplicitMonitoringConfigRepo::new().into(),
                hb_registration_commands,
                hb_registration_commands_sink,
                hb_recovery_events,
                HashMap::new(),
                MockHeartbeatMonitorFactory::new(),
                qos_events,
            );
            let monitor_manager = Arc::new(monitor_manager);
            HealthMonitor::new(monitor_manager)
        };
        let mut publisher = MockMessagePublisher::<VmHealth>::new();
        publisher.expect_publish().returning(|_| Ok(()));

        // ACT
        hm.start(Box::new(publisher), 10);
        hm.should_run.store(false, Ordering::Relaxed);

        // ASSERT
        if let Some(t) = hm.background_thread.lock().unwrap().take() {
            t.join().unwrap();
        } else {
            panic!("Background thread was not found");
        };
    }

    #[tokio::test]
    async fn test_unhealthy_reported_when_hbs_missed() {
        // SETUP
        let unhealthy_hb_monitor =
            TestMonitor { healthy: EntityHealthStatus::Unhealthy, ..TestMonitor::default() };

        let hb_monitors = HashMap::from([(
            ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
            Box::new(unhealthy_hb_monitor) as Box<dyn EntityMonitor>,
        )]);

        let mut publisher = MockMessagePublisher::<VmHealth>::new();
        publisher
            .expect_publish()
            .with(eq(VmHealth {
                all_monitored_service_bundles_healthy: false,
                all_service_bundles_alive: true,
                ..Default::default()
            }))
            .returning(|_| Ok(()));
        let health_reporter = HealthReporter::new(Box::new(publisher));

        let agent_crash_monitors = HashMap::new();
        let sb_recovery_monitors = HashMap::new();

        // ACT
        HealthMonitor::report_state(
            &hb_monitors,
            &sb_recovery_monitors,
            &agent_crash_monitors,
            &health_reporter,
        );

        // ASSERT: see expected VmHealth by in mocked structure
    }

    #[tokio::test]
    async fn test_vm_report_unhealthy_when_agent_crash_monitoring_reports_unhealthy() {
        // SETUP
        let hb_monitors = HashMap::new();

        let unhealthy_agent_monitor =
            TestMonitor { healthy: EntityHealthStatus::Unhealthy, ..TestMonitor::default() };

        let mut publisher = MockMessagePublisher::<VmHealth>::new();
        publisher
            .expect_publish()
            .with(eq(VmHealth {
                all_monitored_service_bundles_healthy: true,
                all_service_bundles_alive: false,
                ..Default::default()
            }))
            .returning(|_| Ok(()));
        let health_reporter = HealthReporter::new(Box::new(publisher));

        let sb_recovery_monitors = HashMap::new();
        let agent_crash_monitors = HashMap::from([(
            "1".to_string(),
            Box::new(unhealthy_agent_monitor) as Box<dyn EntityMonitor>,
        )]);

        // ACT
        HealthMonitor::report_state(
            &hb_monitors,
            &sb_recovery_monitors,
            &agent_crash_monitors,
            &health_reporter,
        );

        // ASSERT: see expected VmHealth by in mocked structure
    }

    #[tokio::test]
    async fn test_vm_report_unhealthy_when_one_sb_recovery_failed() {
        // SETUP
        let hb_monitors = HashMap::new();
        let agent_crash_monitors = HashMap::new();

        let mut publisher = MockMessagePublisher::<VmHealth>::new();
        publisher
            .expect_publish()
            .with(eq(VmHealth {
                all_monitored_service_bundles_healthy: true,
                all_service_bundles_alive: false,
                ..Default::default()
            }))
            .returning(|_| Ok(()));
        let health_reporter = HealthReporter::new(Box::new(publisher));

        let sb_recovery_monitors = HashMap::from([
            (
                ServiceFqin::from_str(UNHEALTHY_SERVICE_FQIN_STR).unwrap(),
                SBOrchestrationState {
                    recovery_state: SBRecoveryState::FailedRecovery,
                    lifecycle_state: SBLifecycleState::Created,
                },
            ),
            (
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
                SBOrchestrationState {
                    recovery_state: SBRecoveryState::Normal,
                    lifecycle_state: SBLifecycleState::Created,
                },
            ),
        ]);

        // ACT
        HealthMonitor::report_state(
            &hb_monitors,
            &sb_recovery_monitors,
            &agent_crash_monitors,
            &health_reporter,
        );

        // ASSERT: see expected VmHealth by in mocked structure
    }

    #[tokio::test]
    async fn test_vm_report_unhealthy_when_sb_recovering() {
        // SETUP
        let hb_monitors = HashMap::new();
        let agent_crash_monitors = HashMap::new();

        let mut publisher = MockMessagePublisher::<VmHealth>::new();
        publisher
            .expect_publish()
            .with(eq(VmHealth {
                all_monitored_service_bundles_healthy: true,
                all_service_bundles_alive: true,
                ..Default::default()
            }))
            .returning(|_| Ok(()));
        let health_reporter = HealthReporter::new(Box::new(publisher));

        let sb_recovery_monitors = HashMap::from([(
            ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
            SBOrchestrationState {
                recovery_state: SBRecoveryState::Recovering,
                lifecycle_state: SBLifecycleState::Created,
            },
        )]);

        // ACT
        HealthMonitor::report_state(
            &hb_monitors,
            &sb_recovery_monitors,
            &agent_crash_monitors,
            &health_reporter,
        );

        // ASSERT: see expected VmHealth by in mocked structure
    }

    #[tokio::test]
    async fn test_vm_report_healthy_when_sb_recovery_monitoring_reports_normal() {
        // SETUP
        let hb_monitors = HashMap::new();
        let agent_crash_monitors = HashMap::new();

        let mut publisher = MockMessagePublisher::<VmHealth>::new();
        publisher
            .expect_publish()
            .with(eq(VmHealth {
                all_monitored_service_bundles_healthy: true,
                all_service_bundles_alive: true,
                ..Default::default()
            }))
            .returning(|_| Ok(()));
        let health_reporter = HealthReporter::new(Box::new(publisher));

        let sb_recovery_monitors = HashMap::from([(
            ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
            SBOrchestrationState {
                recovery_state: SBRecoveryState::Normal,
                lifecycle_state: SBLifecycleState::Created,
            },
        )]);

        // ACT
        HealthMonitor::report_state(
            &hb_monitors,
            &sb_recovery_monitors,
            &agent_crash_monitors,
            &health_reporter,
        );

        // ASSERT: see expected VmHealth by in mocked structure
    }

    #[tokio::test]
    async fn test_list_crashing_service_bundles() {
        let crashing_fqin = ServiceFqin::from_str("vm:com.google.test.Crashing/instance").unwrap();

        let sb_recovery_monitors_map = HashMap::from([
            (
                ServiceFqin::from_str("vm:com.google.test.Running/instance").unwrap(),
                SBOrchestrationState {
                    recovery_state: SBRecoveryState::Normal,
                    lifecycle_state: SBLifecycleState::Started,
                },
            ),
            (
                crashing_fqin.clone(),
                SBOrchestrationState {
                    recovery_state: SBRecoveryState::FailedRecovery,
                    lifecycle_state: SBLifecycleState::Created,
                },
            ),
        ]);

        let (_tx, sb_recovery_monitors) = watch::channel(sb_recovery_monitors_map);
        let (_tx, hb_monitors) = watch::channel(HashMap::new());

        let monitor_manager =
            MonitorManager::new_for_test(sb_recovery_monitors, hb_monitors, HashMap::new());
        let hm = HealthMonitor::new(Arc::new(monitor_manager));

        let request = ListCrashingServiceBundlesRequest { ..Default::default() };

        let response = hm
            .ListCrashingServiceBundles(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                &request,
            )
            .await
            .unwrap();

        assert_eq!(response.fqin, vec![crashing_fqin.to_string()]);
    }

    #[tokio::test]
    async fn test_list_all_service_bundles() {
        let running_fqin = ServiceFqin::from_str("vm:com.google.test.Running/instance").unwrap();
        let created_fqin = ServiceFqin::from_str("vm:com.google.test.Created/instance").unwrap();

        let sb_recovery_monitors_map = HashMap::from([
            (
                running_fqin.clone(),
                SBOrchestrationState {
                    recovery_state: SBRecoveryState::Normal,
                    lifecycle_state: SBLifecycleState::Started,
                },
            ),
            (
                created_fqin.clone(),
                SBOrchestrationState {
                    recovery_state: SBRecoveryState::Normal,
                    lifecycle_state: SBLifecycleState::Created,
                },
            ),
            (
                ServiceFqin::from_str("vm:com.google.test.Crashing/instance").unwrap(),
                SBOrchestrationState {
                    recovery_state: SBRecoveryState::FailedRecovery,
                    lifecycle_state: SBLifecycleState::Created,
                },
            ),
            (
                ServiceFqin::from_str("vm:com.google.test.Destroyed/instance").unwrap(),
                SBOrchestrationState {
                    recovery_state: SBRecoveryState::Normal,
                    lifecycle_state: SBLifecycleState::Destroyed,
                },
            ),
        ]);

        let (_tx, sb_recovery_monitors) = watch::channel(sb_recovery_monitors_map);
        let (_tx, hb_monitors) = watch::channel(HashMap::new());

        let monitor_manager =
            MonitorManager::new_for_test(sb_recovery_monitors, hb_monitors, HashMap::new());
        let hm = HealthMonitor::new(Arc::new(monitor_manager));

        let request = ListAllServiceBundlesRequest { ..Default::default() };

        let response = hm
            .ListAllServiceBundles(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                &request,
            )
            .await
            .unwrap();

        let fqins = response.fqin;
        assert_eq!(fqins.len(), 2);
        assert!(fqins.contains(&running_fqin.to_string()));
        assert!(fqins.contains(&created_fqin.to_string()));
    }

    pub mod utils {
        use std::io::Write;
        pub struct StringWriter {
            pub sink: String,
        }
        impl StringWriter {
            #[allow(clippy::new_without_default)]
            pub fn new() -> Self {
                Self { sink: String::new() }
            }
        }
        impl Write for StringWriter {
            fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
                self.sink.push_str(std::str::from_utf8(buf).unwrap());
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
                unimplemented!("No current tests need this");
            }
        }

        #[test]
        fn test_string_writer() {
            let mut writer = StringWriter::new();
            let _ = write!(writer, "Hello world");
            assert!(writer.sink.as_str() == "Hello world");
        }
    }

    #[tokio::test]
    async fn test_health_monitor_dumpsys_reporting() {
        //setup:
        let hm = {
            let (hb_registration_commands_sink, hb_registration_commands) = mpsc::channel(1);
            let (_, hb_recovery_events) = mpsc::channel(1);
            let (_, qos_events) = mpsc::channel(1);
            let monitor_manager = MonitorManager::new(
                ImplicitMonitoringConfigRepo::new().into(),
                hb_registration_commands,
                hb_registration_commands_sink,
                hb_recovery_events,
                HashMap::new(),
                MockHeartbeatMonitorFactory::new(),
                qos_events,
            );
            let monitor_manager = Arc::new(monitor_manager);
            HealthMonitor::new(monitor_manager)
        };

        let mut writer = StringWriter::new();

        //action:
        hm.dump(&mut writer, &[]).unwrap();

        //checks:
        let title_always_in_dump_report = "INTERNAL STATE";
        let background_thread_chunk = "background_thread running: false";
        let should_run_chunk = "should_run: false";
        assert!(
            writer.sink.as_str().contains(title_always_in_dump_report),
            "Unexpected dump report: {}",
            writer.sink
        );
        assert!(
            writer.sink.as_str().contains(background_thread_chunk),
            "Unexpected dump report: {}",
            writer.sink
        );
        assert!(
            writer.sink.as_str().contains(should_run_chunk),
            "Unexpected dump report: {}",
            writer.sink
        );
    }
}
