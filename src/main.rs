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
//! VM Health Monitor.

use anyhow::anyhow;
use health_api::health_topic::VmHealth;
use log::{debug, error, info};
use rustutils::android::system_properties;
use sdv_comms::sd::ServiceDiscoveryManager;
use sdv_comms::{id::ServiceFqin, ContextRef};
use sdv_dump::DumpBinderConnection;
use sdv_health_monitor::agent_monitor::create_agent_monitor_entries;
use sdv_health_monitor::commstack_trait::CommsImpl;
use sdv_health_monitor::hb_explicit_registration::HBRegistrationListener;
use sdv_health_monitor::hb_implicit_monitoring_db::ImplicitMonitoringConfigRepo;
use sdv_health_monitor::monitor_manager::MonitorManager;
use sdv_health_monitor::power_state::PowerStateReceiverFactory;
use sdv_health_monitor::power_state::PowerStateReceiverFactoryImpl;
use sdv_health_monitor::qos_registration_listener::QosRegistrationListener;
use sdv_health_monitor::sb_recovery_listener::SbRecoveryEventSourceImpl;
use sdv_health_monitor::{
    authorization::AuthorizationImpl, hb_monitor_factory::HeartbeatMonitorFactoryImpl,
    vm_config::Config, HealthMonitor,
};
use sdv_mw_rs_com_android_sdv_health_health_monitor_service_bundle::{
    publisher, HealthMonitorServiceBundle,
};
use sdv_mw_rs_sdv_comms::SdvComms;
use std::future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{event, info_span, Level};
static LOG_TAG: &str = "sdv_hm";

static BINDER_TAG: &str = "hm"; // see agent's SELinux policies
static DUMP_REPORT_TITLE: &str = "SDV Agent dump - Health Monitor";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sdv_log::init_logger(LOG_TAG)?;
    let dm = DumpBinderConnection::initialize(DUMP_REPORT_TITLE, BINDER_TAG);
    dm.set_agent_state("Not initialised");
    let config = Config::get_from_sys_prop(system_properties::read);
    let report_period_ms = Arc::new(config.period_ms);
    dm.add_reporter("VM health report period:", &report_period_ms);
    sdv_tracing::init_tracing();
    binder::ProcessState::start_thread_pool();
    info!("Starting VM Health Monitor...");
    let wait_span = info_span!("health_monitor::wait_for_agents").entered();
    sdv_comms_ctx::wait_for_agents()?;
    wait_span.exit();

    let power_state_receiver_factory: Arc<dyn PowerStateReceiverFactory> =
        Arc::new(PowerStateReceiverFactoryImpl::new());

    let context = create_context()?;
    let id = context.get_self_fqin();
    dm.set_self_fqin(id.to_string());
    dm.set_agent_state("FQIN retrieved");
    let sd = Arc::new(ServiceDiscoveryManager::new(context));

    let agent_monitors = create_agent_monitor_entries(
        config.monitored_binders,
        power_state_receiver_factory.as_ref(),
    );
    let (_sb_recovery_events_source, sb_recovery_events) = SbRecoveryEventSourceImpl::setup();
    let (hb_registration_commands_tx, hb_registration_commands_rx) = mpsc::channel(100);
    let hb_monitor_factory = HeartbeatMonitorFactoryImpl::<CommsImpl>::new(
        context,
        Arc::clone(&sd),
        Arc::new(PowerStateReceiverFactoryImpl::new()),
    );

    let sbr_config_repo = Arc::new(ImplicitMonitoringConfigRepo::new());
    let (_qos_registration, qos_commands) =
        QosRegistrationListener::new::<CommsImpl>(context, sd, Arc::clone(&sbr_config_repo));
    let monitor_manager = MonitorManager::new(
        sbr_config_repo,
        hb_registration_commands_rx,
        hb_registration_commands_tx.clone(), // mpsc, cheap clone
        sb_recovery_events,
        agent_monitors,
        hb_monitor_factory,
        qos_commands,
    );
    let monitor_manager = Arc::new(monitor_manager);
    dm.add_reporter("Recovery monitor manager", &monitor_manager);

    let hm = Arc::new(HealthMonitor::new(monitor_manager));
    dm.add_reporter("Health monitor", &hm);

    let (_mw, vm_health_publisher) = {
        let comms = Arc::new(SdvComms { context });
        let authorization = AuthorizationImpl::new(id);
        let hb_registration_listener =
            Arc::new(HBRegistrationListener::new(hb_registration_commands_tx, authorization));
        let mut mw = HealthMonitorServiceBundle::new(comms, (hb_registration_listener, hm.clone()))
            .await
            .map_err(|e| anyhow!("Could not create middleware bundle: {:?}", e))?;
        let publisher = mw
            .take_publisher(publisher::Variant::<VmHealth>::UNIQUE)
            .ok_or_else(|| anyhow!("Could not take publisher from mw"))?;
        dm.set_agent_state("VM Health Reports publisher created");
        (mw, Box::new(publisher))
    };

    hm.start(vm_health_publisher, config.period_ms);
    dm.set_agent_state("Started");

    info!("Started VM Health Monitor");
    debug!("sdv_health_monitor has started successfully");
    event!(Level::DEBUG, "sdv_health_monitor has started successfully");

    future::pending::<()>().await;
    error!("VM Health Monitor shutdown");
    Ok(())
}

fn create_context() -> anyhow::Result<ContextRef> {
    let fqin_template = ServiceFqin::builder()
        .sdv_vm_name("ignored")
        .sdv_package_name("com.android.sdv.health")
        .service_bundle_name("HealthMonitorServiceBundle")
        .service_instance_name("instance1")
        .build()?;
    Ok(ContextRef::create(fqin_template))
}
