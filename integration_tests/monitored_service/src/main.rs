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

//! This service bundle simulates a monitored service for health monitoring integration tests.
//!
//! It publishes heartbeat messages and its behavior can be controlled via commands sent through standard input.
//! This allows for testing the health monitor's ability to detect healthy and unhealthy states based on the presence or absence of heartbeats.
//!
//! The following commands are supported:
//!
//! * `RegisterConfiguration`: Registers the health configuration with the health monitor.
//! * `DeregisterConfiguration`: Deregisters the health configuration with the health monitor.
//! * `StartHeartbeatsPublishing`: Starts publishing heartbeat messages.
//! * `StopHeartbeatsPublishing`: Stops publishing heartbeat messages.
use std::sync::{Arc, Mutex};
use std::{io::stdin, str::FromStr};

use com_sdv_google_health_health_monitor_registration_service_rpc::Client as HealthMonitorRegistrationServiceClient;
use health_api::health_config::HealthConfiguration;
use health_api::health_monitor_registration_service::*;
use health_api::health_topic::ServiceHeartbeat;
use protobuf::well_known_types::timestamp::Timestamp;
use sdv_comms::id::ServiceFqin;
use sdv_comms::ContextRef;
use sdv_mw_rs_com_sdv_sample_oem_health_monitored_sample_hm_bundle::SampleHMBundle;
use sdv_mw_rs_comms::{Communicate, Publisher, RpcClient};
use sdv_mw_rs_sdv_comms::SdvComms;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::sleep;

const REGISTER_CONFIGURATION_CMD: &str = "RegisterConfiguration";
const DEREGISTER_CONFIGURATION_CMD: &str = "DeregisterConfiguration";
const START_HEARTBEATS_PUBLISHING_CMD: &str = "StartHeartbeatsPublishing";
const STOP_HEARTBEATS_PUBLISHING_CMD: &str = "StopHeartbeatsPublishing";

#[tokio::main]
async fn main() {
    println!("Starting a monitored service bundle");
    println!("Enter the bundle's FQIN");

    let fqin = read_line();
    let fqin = match ServiceFqin::from_str(fqin.as_str()) {
        Ok(x) => x,
        Err(err) => {
            panic!("Failed to parse FQIN {fqin}: {err}");
        }
    };

    let comms = Arc::new(SdvComms { context: ContextRef::create(fqin) });
    let dt_publisher: Mutex<Publisher<ServiceHeartbeat>> = new_dt_publisher(comms.clone()).await;
    let should_publish_heartbeats = Arc::new(AtomicBool::new(false));
    let should_publish_heartbeats_clone = should_publish_heartbeats.clone();

    let health_config = read_health_config();

    tokio::spawn(async move {
        publish_heartbeat_loop(
            dt_publisher,
            should_publish_heartbeats_clone,
            health_config.period_ms.unwrap(),
            health_config.period_ms.unwrap(),
        )
        .await;
    });

    loop {
        let input = read_line();
        match input.as_str() {
            REGISTER_CONFIGURATION_CMD => {
                println!("Registering monitoring configuration");
                register_health_config(comms.clone(), health_config.clone()).await;
            }
            DEREGISTER_CONFIGURATION_CMD => {
                println!("Deregistering configuration");
                deregister_health_config(comms.clone()).await;
            }
            START_HEARTBEATS_PUBLISHING_CMD => {
                println!("Publishing heartbeats");
                should_publish_heartbeats.store(true, Ordering::SeqCst);
            }
            STOP_HEARTBEATS_PUBLISHING_CMD => {
                println!("Stopping heartbeats Publishing");
                should_publish_heartbeats.store(false, Ordering::SeqCst);
            }
            unsupported_cmd => {
                println!(
                    "Unsupported Command {unsupported_cmd}.\nPlease enter any of the following supported commands: \n1. {REGISTER_CONFIGURATION_CMD}\n2. {DEREGISTER_CONFIGURATION_CMD}\n3. {START_HEARTBEATS_PUBLISHING_CMD}\n4. {STOP_HEARTBEATS_PUBLISHING_CMD}"
                );
            }
        }
    }
}

async fn publish_heartbeat_loop(
    dt_publisher: Mutex<Publisher<ServiceHeartbeat>>,
    should_publish: Arc<AtomicBool>,
    initial_delay_ms: u64,
    publish_period: u64,
) {
    sleep(Duration::from_millis(initial_delay_ms)).await;
    loop {
        if should_publish.load(Ordering::SeqCst) {
            let mut shb = ServiceHeartbeat::new();
            let shb_ts = Timestamp::now();
            shb.timestamp = Some(shb_ts).into();
            println!("Timestamp: {}", Timestamp::now().nanos);
            dt_publisher.lock().unwrap().publish(&shb).unwrap();
        }
        sleep(Duration::from_millis(publish_period)).await;
    }
}

async fn new_dt_publisher(comms: Arc<SdvComms>) -> Mutex<Publisher<ServiceHeartbeat>> {
    let mut bundle: SampleHMBundle = SampleHMBundle::new(comms).await.unwrap();
    Mutex::new(bundle
        .take_publisher::<ServiceHeartbeat>(
            sdv_mw_rs_com_sdv_sample_oem_health_monitored_sample_hm_bundle::publisher::Variant::UNIQUE,
        )
        .unwrap())
}

async fn register_health_config(comms: Arc<SdvComms>, health_configuration: HealthConfiguration) {
    let unit_name = sdv_mw_rs_comms::UnitName::builder()
        .vm_name(comms.context.get_self_fqin().get_sdv_vm_name().to_string())
        .package_name("com.sdv.google.health".to_string())
        .bundle_name("HealthMonitorServiceBundle".to_string())
        .service_unit_name(HealthMonitorRegistrationServiceClient::DEFAULT_UNIT_NAME.to_string())
        .build()
        .unwrap();

    let attributes =
        sdv_mw_rs_comms::ClientAttributes::builder().server_name(unit_name).build().unwrap();

    let client = HealthMonitorRegistrationServiceClient::new(
        comms.create_client_transport(&attributes).await.unwrap(),
    );
    client
        .RegisterConfiguration(&RegisterConfigurationRequest {
            config: Some(health_configuration).into(),
            ..Default::default()
        })
        .await
        .unwrap();
}

async fn deregister_health_config(comms: Arc<SdvComms>) {
    let unit_name = sdv_mw_rs_comms::UnitName::builder()
        .vm_name(comms.context.get_self_fqin().get_sdv_vm_name().to_string())
        .package_name("com.sdv.google.health".to_string())
        .bundle_name("HealthMonitorServiceBundle".to_string())
        .service_unit_name(HealthMonitorRegistrationServiceClient::DEFAULT_UNIT_NAME.to_string())
        .build()
        .unwrap();

    let attributes =
        sdv_mw_rs_comms::ClientAttributes::builder().server_name(unit_name).build().unwrap();

    let client = HealthMonitorRegistrationServiceClient::new(
        comms.create_client_transport(&attributes).await.unwrap(),
    );
    client
        .UnregisterConfiguration(&UnregisterConfigurationRequest { ..Default::default() })
        .await
        .unwrap();
}

fn read_health_config() -> HealthConfiguration {
    let initial_delay_ms = read_positive_int("initial_delay_ms");
    let period_ms = read_positive_int("period_ms");
    let num_periods = read_positive_int("num_periods");
    let task_duration_ms = read_positive_int("task_duration_ms");

    HealthConfiguration {
        is_heartbeat_monitored: true,
        initial_delay_ms: Some(initial_delay_ms as u64),
        period_ms: Some(period_ms as u64),
        num_periods: Some(num_periods as u64),
        task_duration_ms: Some(task_duration_ms as u64),
        ..Default::default()
    }
}

fn read_positive_int(parameter_name: &str) -> i32 {
    loop {
        println!("Please enter the {parameter_name}");
        let input = read_line();
        match input.trim().parse::<i32>() {
            Ok(num) if num > 0 => return num,
            _ => println!("Invalid input. Please enter a positive integer."),
        }
    }
}

fn read_line() -> String {
    let mut line = String::new();

    stdin().read_line(&mut line).expect("Failed to read from standard input");

    line.trim().to_string()
}
