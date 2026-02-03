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
//! Integration tests for the health monitor.

mod vm_monitoring_service;

#[cfg(test)]
mod tests {
    use health_api::health_monitor_config::HealthMonitorConfiguration;
    use log::info;
    use sdv_comms::id::ServiceFqin;
    use std::io::Write;
    use std::time::Duration;

    use crate::vm_monitoring_service::VmMonitoringService;
    use health_api::health_monitor_service::{
        ListUnhealthyMonitoredServiceBundlesRequest, ListUnhealthyMonitoredServiceBundlesResponse,
    };
    use health_api::health_topic::VmHealth;
    use protobuf::text_format;
    use rustutils::android::system_properties::read;
    use std::process::{Child, Command, Stdio};

    /// Default configuration path for the HM config.
    const HM_CONFIG_PATH_ENV_VAR: &str = "persist.sdv.health_monitor.config_path";
    const SERVICE_BUNDLE_PACKAGE_NAME: &str = "com.sdv.sample_oem.health.monitored";
    const SECOND_SAMPLE_SERVICE_BUNDLE_NAME: &str = "SecondSampleHMBundle";
    const SAMPLE_SERVICE_BUNDLE_NAME: &str = "SampleHMBundle";
    const SERVICE_BUNDLE_INSTANCE_NAME: &str = "test1";
    use lazy_static::lazy_static;

    lazy_static! {
        static ref SAMPLE_HM_BUNDLE_FQIN: ServiceFqin = ServiceFqin::builder()
            .sdv_vm_name("instance1")
            .sdv_package_name(SERVICE_BUNDLE_PACKAGE_NAME)
            .service_bundle_name(SAMPLE_SERVICE_BUNDLE_NAME)
            .service_instance_name(SERVICE_BUNDLE_INSTANCE_NAME)
            .build()
            .expect("Failed to create Fqin");
        static ref SECOND_SAMPLE_HM_BUNDLE_FQIN: ServiceFqin = ServiceFqin::builder()
            .sdv_vm_name("instance1")
            .sdv_package_name(SERVICE_BUNDLE_PACKAGE_NAME)
            .service_bundle_name(SECOND_SAMPLE_SERVICE_BUNDLE_NAME)
            .service_instance_name(SERVICE_BUNDLE_INSTANCE_NAME)
            .build()
            .expect("Failed to create Fqin");
    }

    #[tokio::test]
    /// Tests monitoring two services.  This test verifies that the health monitor correctly identifies
    /// healthy and unhealthy service bundles based on heartbeat messages. It simulates two monitored
    /// service bundles, starts and stops their heartbeat publishing to trigger healthy and unhealthy
    /// states, and then asserts that the health monitor reports the correct status via VM health
    /// reports and the `ListUnhealthyMonitoredServiceBundles` RPC.
    async fn monitoring_two_services_succeeds() {
        // Needed for multipub subscriber in VmMonitoringService to function.
        binder::ProcessState::start_thread_pool();

        sdv_log::init_logger("sdv_hm_integration_test").expect("Logger initialisation never fails");

        let mut service_bundle_1 = monitored_service_cmd();

        info!("Verifying initial VM health reports VM is healthy.");
        let vm_monitor: VmMonitoringService = VmMonitoringService::new().await;

        send_commad(&mut service_bundle_1, &SAMPLE_HM_BUNDLE_FQIN.to_string());
        send_commad(
            &mut service_bundle_1,
            "50", // initial_delay_ms
        );
        send_commad(
            &mut service_bundle_1,
            "5", // task_duration_ms
        );
        send_commad(
            &mut service_bundle_1,
            "1", // num_periods
        );
        send_commad(
            &mut service_bundle_1,
            "5", // period_ms
        );

        send_commad(&mut service_bundle_1, "RegisterConfiguration");
        send_commad(&mut service_bundle_1, "StartHeartbeatsPublishing");

        // The Health Monitor agent publishes VM health reports less frequently than monitored services publish heartbeats.
        // Timeouts for waiting on these reports should be based on the report frequency (defined in the HM config) to prevent spurious test failures.
        // We use a timeout of 1.5 times the configured reporting period to add a safety margin.
        let expected_vm_report_timeout = (hm_agent_config().period_ms as f64 * 1.5) as u64;
        wait_for_expected_vm_report(
            &vm_monitor,
            &VmHealth {
                all_monitored_service_bundles_healthy: true,
                all_service_bundles_alive: true,
                ..Default::default()
            },
            expected_vm_report_timeout,
        )
        .expect("Failed to find expected VmHealth report");

        let mut service_bundle_2 = monitored_service_cmd();
        send_commad(&mut service_bundle_2, &SECOND_SAMPLE_HM_BUNDLE_FQIN.to_string());

        send_commad(
            &mut service_bundle_2,
            "50", // initial_delay_ms
        );
        send_commad(
            &mut service_bundle_2,
            "10", // task_duration_ms
        );
        send_commad(
            &mut service_bundle_2,
            "1", // num_periods
        );
        send_commad(
            &mut service_bundle_2,
            "10", // period_ms
        );

        send_commad(&mut service_bundle_2, "RegisterConfiguration");
        send_commad(&mut service_bundle_2, "StartHeartbeatsPublishing");

        info!("Verifying VM health reports all monitored service bundles are healthy.");
        wait_for_expected_vm_report(
            &vm_monitor,
            &VmHealth {
                all_monitored_service_bundles_healthy: true,
                all_service_bundles_alive: true,
                ..Default::default()
            },
            expected_vm_report_timeout,
        )
        .expect("Failed to find expected VmHealth report");

        info!("Stopping first service bundle heartbeats publishing");
        // Stop publishing heartbeats for the first monitored service bundle to simulate an unhealthy state.
        send_commad(&mut service_bundle_1, "StopHeartbeatsPublishing");

        info!("Waiting for bad vm report");
        // Wait for the Health Monitor to detect the unhealthy state and publish an updated VM report.
        wait_for_expected_vm_report(
            &vm_monitor,
            &VmHealth {
                all_monitored_service_bundles_healthy: false,
                all_service_bundles_alive: true,
                ..Default::default()
            },
            expected_vm_report_timeout,
        )
        .expect("Failed to find expected VmHealth report");

        info!(
        "Checking that ListUnhealthyMonitoredServiceBundles returns the expected unhealthy service"
    );
        // Query the Health Monitor for the list of unhealthy service bundles and verify that only
        // the first monitored service bundle is reported as unhealthy.
        let unhealthy_services: ListUnhealthyMonitoredServiceBundlesResponse = vm_monitor
            .hm_client
            .ListUnhealthyMonitoredServiceBundles(
                &ListUnhealthyMonitoredServiceBundlesRequest::new(),
            )
            .await
            .expect("Failed to list unhealthy services");

        assert_eq!(unhealthy_services.fqin.len(), 1);
        assert_eq!(unhealthy_services.fqin[0], SAMPLE_HM_BUNDLE_FQIN.to_string(),);

        info!("Resuming first service bundle heartbeats publishing");
        send_commad(&mut service_bundle_1, "StartHeartbeatsPublishing");

        info!("Waiting for vm to recover");
        wait_for_expected_vm_report(
            &vm_monitor,
            &VmHealth {
                all_monitored_service_bundles_healthy: true,
                all_service_bundles_alive: true,
                ..Default::default()
            },
            expected_vm_report_timeout,
        )
        .expect("Failed to find expected VmHealth report");

        info!("Stopping second service bundle heartbeats publishing");
        send_commad(&mut service_bundle_2, "StopHeartbeatsPublishing");

        info!("Waiting for vm to fail again");
        wait_for_expected_vm_report(
            &vm_monitor,
            &VmHealth {
                all_monitored_service_bundles_healthy: false,
                all_service_bundles_alive: true,
                ..Default::default()
            },
            expected_vm_report_timeout,
        )
        .expect("Failed to find expected VmHealth report");

        info!(
        "Checking that ListUnhealthyMonitoredServiceBundles returns the expected unhealthy service"
    );
        let unhealthy_services: ListUnhealthyMonitoredServiceBundlesResponse = vm_monitor
            .hm_client
            .ListUnhealthyMonitoredServiceBundles(
                &ListUnhealthyMonitoredServiceBundlesRequest::new(),
            )
            .await
            .expect("Failed to list unhealthy services");

        assert_eq!(unhealthy_services.fqin.len(), 1);
        assert_eq!(unhealthy_services.fqin[0], SECOND_SAMPLE_HM_BUNDLE_FQIN.to_string(),);

        info!("Resuming second service bundle heartbeats publishing");
        send_commad(&mut service_bundle_2, "StartHeartbeatsPublishing");

        info!("Waiting for vm to recover again");
        wait_for_expected_vm_report(
            &vm_monitor,
            &VmHealth {
                all_monitored_service_bundles_healthy: true,
                all_service_bundles_alive: true,
                ..Default::default()
            },
            expected_vm_report_timeout,
        )
        .expect("Failed to find expected VmHealth report");

        info!("Checking that ListUnhealthyMonitoredServiceBundles returns no services");
        let unhealthy_services: ListUnhealthyMonitoredServiceBundlesResponse = vm_monitor
            .hm_client
            .ListUnhealthyMonitoredServiceBundles(
                &ListUnhealthyMonitoredServiceBundlesRequest::new(),
            )
            .await
            .expect("Failed to list unhealthy services");
        assert_eq!(unhealthy_services.fqin.len(), 0);

        info!("Test Passed");
        service_bundle_1.kill().expect("Failed to kill");
        service_bundle_2.kill().expect("Failed to kill");
    }

    fn wait_for_expected_vm_report(
        vm_monitor: &VmMonitoringService,
        expected_vm_health: &VmHealth,
        millis_timeout: u64,
    ) -> Result<(), String> {
        let start_time = std::time::Instant::now();

        loop {
            let vm_health_reports =
                vm_monitor.dt_subscriber.read_next_messages(100).expect("Failed to read from DT");

            if vm_health_reports.iter().any(|vm_health| vm_health == expected_vm_health) {
                break;
            }

            std::thread::sleep(Duration::from_millis(1));

            if start_time.elapsed().as_millis() > millis_timeout as u128 {
                return Err(format!("Timeout exceeded while waiting for expected VM health report.  Waited for {millis_timeout}ms"));
            }
        }

        Ok(())
    }

    fn hm_agent_config() -> HealthMonitorConfiguration {
        let config_file_path = match read(HM_CONFIG_PATH_ENV_VAR) {
            Ok(Some(x)) => x,
            Ok(None) => {
                panic!("{HM_CONFIG_PATH_ENV_VAR} system property not set");
            }
            Err(e) => {
                panic!("Error reading {HM_CONFIG_PATH_ENV_VAR}: {e}");
            }
        };
        info!("Reading configuration from {config_file_path:?}");
        let config_textproto = std::fs::read_to_string(config_file_path)
            .expect("Failed to read health monitor configuration");

        text_format::parse_from_str::<HealthMonitorConfiguration>(&config_textproto)
            .expect("Failed to read health monitor configuration")
    }

    fn send_commad(child: &mut Child, command: &str) {
        let command = format!("{command}\n");
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(command.as_bytes())
            .expect("Failed to write to stdin");
        child.stdin.as_mut().unwrap().flush().expect("Failed to flush stdio");
    }

    /// Starts the monitored service process and returns a handle to it.
    fn monitored_service_cmd() -> std::process::Child {
        Command::new("sdv_integration_test_health_monitored_service")
            .stdin(Stdio::piped()) // Pipe stdin to the child process
            .spawn()
            .expect("Failed to start sdv_integration_test_health_monitored_service")
    }
}
