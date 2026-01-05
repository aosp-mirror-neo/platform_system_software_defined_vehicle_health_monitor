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

use crate::hb_explicit_registration::HBRegistrationCommand;
pub use crate::health_reporter::MessagePublisher;
use crate::monitor_manager::MonitorManager;
use crate::monitor_model::EntityHealthStatus;
use crate::monitor_model::EntityMonitor;
use crate::monitor_model::MonitorId;
use crate::monitor_model::SBRecoveryState;
use async_trait::async_trait;
use authorization::AuthorizationImpl;
pub use com_sdv_google_health_health_monitor_service_rpc::Interface as HealthMonitorService;
use hb_explicit_registration::RegistrationCommandError;
use hb_monitor_factory::HeartbeatMonitorFactory;
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
pub use sdv_dump::DumpBinderConnection;
use sdv_dump::DumpStateReporter;
use std::ops::DerefMut;
use std::sync::Mutex;
use std::time::Duration;
use std::{
    collections::HashMap,
    io::Error,
    ops::Add,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::Receiver,
        Arc,
    },
    thread,
    time::Instant,
};
use tracing::instrument;

pub mod agent_monitor;
pub mod authorization;
mod hb_config;
pub mod hb_explicit_registration;
pub mod hb_monitor;
pub mod hb_monitor_factory;
mod health_reporter;
mod implicit_monitoring_db;
pub mod monitor_manager;
pub mod monitor_model;
pub mod power_state;
pub mod read_sys_prop;
pub mod sb_recovery_listener;
pub mod vm_config;

pub struct HealthMonitor {
    background_thread: Mutex<Option<thread::JoinHandle<()>>>,
    // TODO: move (service_bundle_monitors) HB monitoring to monitor_manager
    service_bundle_monitors: Arc<Mutex<HashMap<MonitorId, Box<dyn EntityMonitor>>>>,
    should_run: Arc<AtomicBool>,
}

impl HealthMonitor {
    pub fn new() -> Self {
        Self {
            background_thread: Mutex::new(None),
            service_bundle_monitors: Arc::new(Mutex::new(HashMap::new())),
            should_run: Arc::new(AtomicBool::new(false)),
        }
    }

    #[instrument(skip(self, publisher, recovery_monitor_manager))]
    pub fn start(
        &self,
        publisher: Box<dyn MessagePublisher<VmHealth>>,
        report_period_ms: u64,
        request_rx: Receiver<HBRegistrationCommand>,
        recovery_monitor_manager: Arc<MonitorManager>,
    ) {
        self.should_run.store(true, Ordering::SeqCst);

        let should_run = self.should_run.clone();
        let service_bundle_monitors = self.service_bundle_monitors.clone();

        *self.background_thread.lock().unwrap() = Some(thread::spawn(move || {
            let health_reporter = HealthReporter::new(publisher);

            while should_run.load(Ordering::SeqCst) {
                let next_report_time = Instant::now().add(Duration::from_millis(report_period_ms));

                // Report overall VM health
                Self::report_state(
                    service_bundle_monitors.as_ref(),
                    &recovery_monitor_manager.sb_recovery_monitors.borrow(),
                    &recovery_monitor_manager.agent_crash_monitors,
                    &health_reporter,
                );

                // Process pending registration commands:
                Self::process_request_queue(
                    service_bundle_monitors.lock().unwrap().deref_mut(),
                    &request_rx,
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
        heartbeat_monitors: &Mutex<HashMap<MonitorId, Box<dyn EntityMonitor>>>,
        sb_recovery_monitors: &HashMap<MonitorId, SBRecoveryState>,
        agent_crash_monitors: &HashMap<MonitorId, Box<dyn EntityMonitor>>,
        health_reporter: &HealthReporter,
    ) {
        let hbs_missed = heartbeat_monitors
            .lock()
            .unwrap()
            .values()
            .any(|m| m.get_health_status() == EntityHealthStatus::Unhealthy);

        let agent_recoveries_failed = agent_crash_monitors
            .values()
            .any(|m| m.get_health_status() == EntityHealthStatus::Unhealthy);
        let sb_recoveries_failed = sb_recovery_monitors
            .values()
            .any(|m| m.get_health_status() == EntityHealthStatus::Unhealthy);

        let recoveries_failed = agent_recoveries_failed || sb_recoveries_failed;
        if let Err(err) = health_reporter.report_vm_health(hbs_missed, recoveries_failed) {
            error!("Failed to report vm health: {err}");
        }
    }

    fn process_request_queue(
        monitors: &mut HashMap<MonitorId, Box<dyn EntityMonitor>>,
        request_rx: &Receiver<HBRegistrationCommand>,
    ) {
        while let Ok(request) = request_rx.try_recv() {
            match request {
                HBRegistrationCommand::Register(fqin, new_monitor, response_tx) => {
                    let id = MonitorId::BundleMonitorId(fqin);
                    // TODO: b/409710102 - Adjust logic once subscriber starts to report an error on publisher crash.
                    if let Some(existing_monitor) = monitors.get(&id) {
                        if *existing_monitor.get_config() == *new_monitor.get_config() {
                            warn!("Unregister and re-register {id:?} from health monitoring as it probably crashed");
                            monitors.insert(id, new_monitor);
                            response_tx.send(Ok(())).unwrap_or_else(|_| {
                                warn!(
                                    "Re-registration case, oneshot receiver unexpectedly dropped!"
                                )
                            });
                        } else {
                            warn!("{id:?} is already registered for health monitoring");
                            response_tx
                                .send(Err(RegistrationCommandError::AlreadyRegistered))
                                .unwrap_or_else(|_|
                                    warn!("Already registered case, oneshot receiver unexpectedly dropped!")
                                );
                        }
                    } else {
                        monitors.insert(id, new_monitor);
                        response_tx.send(Ok(())).unwrap_or_else(|_| {
                            warn!("Normal registration, oneshot receiver unexpectedly dropped!")
                        });
                    }
                }
                HBRegistrationCommand::Unregister(fqin, response_tx) => {
                    monitors.remove(&MonitorId::BundleMonitorId(fqin));
                    response_tx.send(()).unwrap_or_else(|_| {
                        warn!("Unregistration, oneshot receiver unexpectedly dropped!")
                    });
                }
            }
        }
    }

    #[instrument(skip(self))]
    fn stop(&mut self) {
        self.should_run.store(false, Ordering::SeqCst);
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
impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new()
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
        Err(sdv_status_rs::SdvStatus::with_message(
            sdv_status_rs::SdvStatusCode::Unimplemented,
            "Health Monitor doesn't support ListAllServiceBundles, it will be implemented in future"))
    }

    #[instrument(skip(self))]
    async fn ListCrashingServiceBundles(
        &self,
        _fqin: sdv_comms::id::ServiceFqin,
        _request: &ListCrashingServiceBundlesRequest,
    ) -> sdv_status_rs::SdvResult<ListCrashingServiceBundlesResponse> {
        Err(sdv_status_rs::SdvStatus::with_message(
            sdv_status_rs::SdvStatusCode::Unimplemented,
            "Health Monitor doesn't support ListCrashingServiceBundles, it will be implemented in future"))
    }

    #[instrument(skip(self))]
    async fn ListUnhealthyMonitoredServiceBundles(
        &self,
        _fqin: sdv_comms::id::ServiceFqin,
        _request: &ListUnhealthyMonitoredServiceBundlesRequest,
    ) -> sdv_status_rs::SdvResult<ListUnhealthyMonitoredServiceBundlesResponse> {
        let unhealthy_services: Vec<String> = self
            .service_bundle_monitors
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, value)| value.get_health_status() == EntityHealthStatus::Unhealthy)
            .filter_map(|(key, _)| {
                if let MonitorId::BundleMonitorId(id) = key {
                    Some(id.to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(ListUnhealthyMonitoredServiceBundlesResponse {
            fqin: unhealthy_services,
            ..Default::default()
        })
    }
}

impl DumpStateReporter for HealthMonitor {
    fn dump(&self, writer: &mut dyn std::io::Write, args: &[&std::ffi::CStr]) -> Result<(), Error> {
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
        )?;
        writeln!(writer, "\nMONITORING INTERNAL STATE:")?;
        if self.service_bundle_monitors.lock().unwrap().is_empty() {
            writeln!(writer, "NO ACTIVE MONITORS")?;
        }
        self.service_bundle_monitors.lock().unwrap().iter().enumerate().try_for_each(
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
        Result::Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::health_reporter::MockMessagePublisher;
    use crate::tests::utils::StringWriter;
    use hb_config::HeartbeatMonitoringConfig;
    use mockall::mock;
    use mockall::predicate::eq;
    use sdv_comms::id::ServiceFqin;
    use sdv_status_rs::SdvStatusCode;
    use std::ffi::CStr;
    use std::io::{Error, Write};
    use std::str::FromStr;
    use tokio::sync::mpsc;

    mock! {
        pub EntityMonitor {} // mock name: MockEntityMonitor
        impl EntityMonitor for EntityMonitor{
            fn get_config(&self) -> &HeartbeatMonitoringConfig;
            fn get_health_status(&self) -> EntityHealthStatus;

        }
        impl DumpStateReporter for EntityMonitor{
            fn dump<'a>(&self, writer: &mut dyn Write, args: &[&'a CStr]) -> std::result::Result<(), Error>;
        }
    }

    static HEALTHY_SERVICE_FQIN_STR: &str = "vm:com.google.sdv.HealthyServiceBundle/sample";
    static UNHEALTHY_SERVICE_FQIN_STR: &str = "vm:com.google.sdv.UnhealthyServiceBundle/sample";
    static DEFAULT_CONFIG: HeartbeatMonitoringConfig = HeartbeatMonitoringConfig {
        initial_delay_ms: 100u64,
        period_ms: 200u64,
        num_periods: 3u64,
        task_duration_ms: 40u64,
    };

    #[tokio::test]
    async fn test_start_starts_background_thread_and_can_be_stopped() {
        // SETUP
        let hm = HealthMonitor::default();
        let (_request_tx, request_rx) = std::sync::mpsc::channel();
        let mut publisher = MockMessagePublisher::<VmHealth>::new();
        publisher.expect_publish().returning(|_| Ok(()));

        let (_, empty_rx) = mpsc::channel(1);
        let monitor_manager = MonitorManager::new(empty_rx, HashMap::new());

        // ACT
        hm.start(Box::new(publisher), 10, request_rx, monitor_manager.into());
        hm.should_run.store(false, Ordering::Relaxed);

        // ASSERT
        if let Some(t) = hm.background_thread.lock().unwrap().take() {
            t.join().unwrap();
        } else {
            panic!("Background thread was not found");
        };
    }

    #[tokio::test]
    async fn test_double_register_command_returning_an_error_second_time() {
        // setup: 2 registration requests: same fqin, but different configs ->
        // the current logic considers this a case of double registration
        let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();

        let config1 = DEFAULT_CONFIG.clone();
        let config2 = HeartbeatMonitoringConfig { initial_delay_ms: 0xCAFEu64, ..DEFAULT_CONFIG };
        let mut monitor1 = MockEntityMonitor::new();
        let mut monitor2 = MockEntityMonitor::new();
        monitor1.expect_get_config().return_const(config1.clone());
        monitor2.expect_get_config().return_const(config2.clone());

        let mut monitors = HashMap::new();
        let (request_tx, request_rx) = std::sync::mpsc::channel();
        let (oneshot1_tx, oneshot1_rx) = tokio::sync::oneshot::channel();
        let (oneshot2_tx, oneshot2_rx) = tokio::sync::oneshot::channel();

        request_tx
            .send(HBRegistrationCommand::Register(
                Box::new(fqin.clone()),
                Box::new(monitor1),
                oneshot1_tx,
            ))
            .unwrap();
        request_tx
            .send(HBRegistrationCommand::Register(Box::new(fqin), Box::new(monitor2), oneshot2_tx))
            .unwrap();

        HealthMonitor::process_request_queue(&mut monitors, &request_rx);
        assert!(oneshot1_rx.await.unwrap().is_ok());
        assert!(oneshot2_rx.await.unwrap().is_err());
        assert!(monitors.len() == 1);
    }

    #[tokio::test]
    async fn test_register_configuration_when_dead_bundle_succeeds() {
        // setup: 2 registration requests: same fqin, same config ->
        // the current logic considers this a case of valid reregistration, as bundle might have crashed
        let fqin = ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap();

        let config = DEFAULT_CONFIG.clone();
        let mut monitor1 = MockEntityMonitor::new();
        let mut monitor2 = MockEntityMonitor::new();
        monitor1.expect_get_config().return_const(config.clone());
        monitor2.expect_get_config().return_const(config.clone());

        let mut monitors = HashMap::new();
        let (request_tx, request_rx) = std::sync::mpsc::channel();
        let (oneshot1_tx, oneshot1_rx) = tokio::sync::oneshot::channel();
        let (oneshot2_tx, oneshot2_rx) = tokio::sync::oneshot::channel();

        request_tx
            .send(HBRegistrationCommand::Register(
                Box::new(fqin.clone()),
                Box::new(monitor1),
                oneshot1_tx,
            ))
            .unwrap();
        request_tx
            .send(HBRegistrationCommand::Register(Box::new(fqin), Box::new(monitor2), oneshot2_tx))
            .unwrap();

        HealthMonitor::process_request_queue(&mut monitors, &request_rx);
        assert!(oneshot1_rx.await.unwrap().is_ok());
        assert!(oneshot2_rx.await.unwrap().is_ok());
        assert!(monitors.len() == 1);
    }

    #[tokio::test]
    async fn test_list_unhealthy_monitored_service_bundles_post_monitor_registration() {
        let mut hm = HealthMonitor::default();

        let (register_commands_tx, register_commands_rx) = std::sync::mpsc::channel();
        let mut healthy_service_monitor = MockEntityMonitor::new();
        healthy_service_monitor
            .expect_get_health_status()
            .returning(|| EntityHealthStatus::Healthy);
        let (healthy_oneshot_tx, healthy_oneshot_rx) = tokio::sync::oneshot::channel();
        register_commands_tx
            .send(HBRegistrationCommand::Register(
                Box::new(ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap()),
                Box::new(healthy_service_monitor),
                healthy_oneshot_tx,
            ))
            .unwrap();

        let mut unhealthy_service_monitor = MockEntityMonitor::new();
        unhealthy_service_monitor
            .expect_get_health_status()
            .returning(|| EntityHealthStatus::Unhealthy);
        let (unhealthy_oneshot_tx, unhealthy_oneshot_rx) = tokio::sync::oneshot::channel();
        register_commands_tx
            .send(HBRegistrationCommand::Register(
                Box::new(ServiceFqin::from_str(UNHEALTHY_SERVICE_FQIN_STR).unwrap()),
                Box::new(unhealthy_service_monitor),
                unhealthy_oneshot_tx,
            ))
            .unwrap();

        let mut publisher = MockMessagePublisher::<VmHealth>::new();
        publisher.expect_publish().returning(|_| Ok(()));

        HealthMonitor::process_request_queue(
            hm.service_bundle_monitors.lock().unwrap().deref_mut(),
            &register_commands_rx,
        );

        assert!(hm.service_bundle_monitors.lock().unwrap().len() == 2);
        assert!(healthy_oneshot_rx.await.is_ok());
        assert!(unhealthy_oneshot_rx.await.is_ok());

        let request = ListUnhealthyMonitoredServiceBundlesRequest::new();
        let response = hm
            .ListUnhealthyMonitoredServiceBundles(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                &request,
            )
            .await
            .unwrap();

        assert_eq!(response.fqin.len(), 1);
        assert_eq!(response.fqin[0], UNHEALTHY_SERVICE_FQIN_STR);

        hm.stop();
    }

    #[tokio::test]
    async fn test_report_state_reports_all_healthy_post_monitor_registration() {
        // SETUP
        let mut hm = HealthMonitor::default();

        let (register_commands_tx, register_commands_rx) = std::sync::mpsc::channel();
        let mut healthy_service_monitor = MockEntityMonitor::new();
        healthy_service_monitor
            .expect_get_health_status()
            .times(1)
            .returning(|| EntityHealthStatus::Healthy);
        let (healthy_oneshot_tx, healthy_oneshot_rx) = tokio::sync::oneshot::channel();
        register_commands_tx
            .send(HBRegistrationCommand::Register(
                Box::new(ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap()),
                Box::new(healthy_service_monitor),
                healthy_oneshot_tx,
            ))
            .unwrap();

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

        let agent_crash_monitors = HashMap::new();
        let sb_recovery_monitors = HashMap::new();

        // ACT
        HealthMonitor::process_request_queue(
            hm.service_bundle_monitors.lock().unwrap().deref_mut(),
            &register_commands_rx,
        );
        HealthMonitor::report_state(
            hm.service_bundle_monitors.as_ref(),
            &sb_recovery_monitors,
            &agent_crash_monitors,
            &health_reporter,
        );
        hm.stop();

        // ASSERT (see also mockall checks declared in SETUP)
        assert!(healthy_oneshot_rx.await.is_ok())
    }

    #[tokio::test]
    async fn test_unhealthy_reported_wheb_hbs_missed() {
        // SETUP
        let mut healthy_service_monitor = MockEntityMonitor::new();
        healthy_service_monitor
            .expect_get_health_status()
            .times(1)
            .returning(|| EntityHealthStatus::Unhealthy);
        let hb_monitors = Mutex::new(HashMap::from([(
            MonitorId::BundleMonitorId(Box::new(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
            )),
            Box::new(healthy_service_monitor) as Box<dyn EntityMonitor>,
        )]));

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
    async fn test_unhealthy_state_reported_when_agent_crash_monitoring_reports_unhealthy() {
        // SETUP
        let hb_monitors = Mutex::new(HashMap::new());

        let mut unhealthy_agent_monitor = MockEntityMonitor::new();
        unhealthy_agent_monitor
            .expect_get_health_status()
            .times(1)
            .returning(|| EntityHealthStatus::Unhealthy);

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
            MonitorId::AgentMonitorId("1".to_string()),
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
    async fn test_unhealthy_state_reported_when_one_sb_recovery_failed() {
        // SETUP
        let hb_monitors = Mutex::new(HashMap::new());
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
                MonitorId::BundleRecoveryMonitorId(Box::new(
                    ServiceFqin::from_str(UNHEALTHY_SERVICE_FQIN_STR).unwrap(),
                )),
                SBRecoveryState::FailedRecovery,
            ),
            (
                MonitorId::BundleRecoveryMonitorId(Box::new(
                    ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
                )),
                SBRecoveryState::Normal,
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
    async fn test_healthy_state_reported_when_sb_recovering() {
        // SETUP
        let hb_monitors = Mutex::new(HashMap::new());
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
            MonitorId::BundleRecoveryMonitorId(Box::new(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
            )),
            SBRecoveryState::Recovering,
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
    async fn test_healthy_state_reported_when_sb_recovery_monitoring_reports_normal() {
        // SETUP
        let hb_monitors = Mutex::new(HashMap::new());
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
            MonitorId::BundleRecoveryMonitorId(Box::new(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap(),
            )),
            SBRecoveryState::Normal,
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
    async fn test_list_crashes_unimplemented() {
        let hm = HealthMonitor::default();

        let request = ListCrashingServiceBundlesRequest { ..Default::default() };

        let response = hm
            .ListCrashingServiceBundles(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                &request,
            )
            .await;

        assert!(response.is_err());
        assert_eq!(response.unwrap_err().status_code(), SdvStatusCode::Unimplemented);
    }

    #[tokio::test]
    async fn test_list_all_services_unimplemented() {
        let hm = HealthMonitor::default();

        let request = ListAllServiceBundlesRequest { ..Default::default() };

        let response = hm
            .ListAllServiceBundles(
                ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).expect("Valid Fqin"),
                &request,
            )
            .await;

        assert!(response.is_err());
        assert_eq!(response.unwrap_err().status_code(), SdvStatusCode::Unimplemented);
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

    #[test]
    fn test_health_reporter_correctly_reports_internal_state_via_dumpsys() {
        //setup:
        let mut writer = StringWriter::new();

        let fqin1_name = "vm:com.google.sdv.MyBundle/instance1";
        let fqin2_name = "vm:com.google.sdv.MyBundle/instance2";
        let fqin1 = ServiceFqin::from_str(fqin1_name).expect("Valid Fqin");
        let fqin2 = ServiceFqin::from_str(fqin2_name).expect("Valid Fqin");
        let monitor1_expected_dump = "DUMP FROM MONITOR1";
        let monitor2_expected_dump = "DUMP FROM MONITOR2";

        let mut mock_monitor_1 = MockEntityMonitor::new();
        mock_monitor_1
            .expect_dump()
            .returning(move |writer, _| write!(writer, "{}", &monitor1_expected_dump));

        let mut mock_monitor_2 = MockEntityMonitor::new();
        mock_monitor_2
            .expect_dump()
            .returning(move |writer, _| write!(writer, "{}", &monitor2_expected_dump));

        let hm = HealthMonitor::default();

        let _ = hm
            .service_bundle_monitors
            .lock()
            .unwrap()
            .insert(MonitorId::BundleMonitorId(Box::new(fqin1)), Box::new(mock_monitor_1));
        let _ = hm
            .service_bundle_monitors
            .lock()
            .unwrap()
            .insert(MonitorId::BundleMonitorId(Box::new(fqin2)), Box::new(mock_monitor_2));

        //action:
        let _ = hm.dump(&mut writer, &[]);

        //checks:
        let title_always_in_dump_report = "INTERNAL STATE";
        assert!(writer.sink.as_str().contains(title_always_in_dump_report));
        assert!(writer.sink.as_str().contains(monitor1_expected_dump));
        assert!(writer.sink.as_str().contains(monitor2_expected_dump));
    }

    #[test]
    fn test_registration_disconnect_does_not_lead_to_panic_b439620660() {
        // setup
        let mut hm = HealthMonitor::default();
        let (register_commands_tx, register_commands_rx) = std::sync::mpsc::channel();
        let healthy_service_monitor = MockEntityMonitor::new();
        let (registration_result_tx, registration_result_rx) = tokio::sync::oneshot::channel();
        register_commands_tx
            .send(HBRegistrationCommand::Register(
                Box::new(ServiceFqin::from_str(HEALTHY_SERVICE_FQIN_STR).unwrap()),
                Box::new(healthy_service_monitor),
                registration_result_tx,
            ))
            .unwrap();

        // simulate registration cancelled early:
        std::mem::drop(registration_result_rx);

        // should not panic:
        HealthMonitor::process_request_queue(
            hm.service_bundle_monitors.lock().unwrap().deref_mut(),
            &register_commands_rx,
        );

        // cleanup
        hm.stop();
    }
}
