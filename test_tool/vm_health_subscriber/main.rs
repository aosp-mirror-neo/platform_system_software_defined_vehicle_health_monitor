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
//! Lightweight vm health report subscriber. Used for performance testing

use anyhow::anyhow;
use futures::task::Poll;
use futures::Future;
use futures::StreamExt;
use health_api::health_topic::VmHealth;
use log::{error, info};
use sdv::comms::dt::SubscriberOptions;
use sdv::comms::dt::SubscriberStream;
use sdv::comms::id::ServiceFqin;
use sdv::comms::sd::ServiceDiscoveryManager;
use sdv::comms::sd::ServiceUnitChangeEvent::Registered;
use sdv::comms::sd::ServiceUnitDefinition;
use sdv::comms::sd::UnitType;
use sdv::comms::ContextRef;
use sdv_mw_rs_com_android_sdv_health_health_monitor_service_bundle::UnitType as MwUnitType;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Waker;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::event;
use tracing::Level;

const LOG_TAG: &str = "vm_health_sub";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    binder::ProcessState::start_thread_pool();
    sdv_comms::wait_for_agents()?;
    sdv_tracing::init_tracing();
    sdv_log::init_logger(LOG_TAG)?;

    let fqin = ServiceFqin::builder()
        .sdv_vm_name("ignored")
        .sdv_package_name("com.android.sdv.sample.oem.health.monitoring")
        .service_bundle_name("VmHMBundle")
        .service_instance_name("instance1")
        .build()
        .unwrap();
    let ctx = ContextRef::create(fqin);

    discover_health_reporters_and_launch_listeners(ctx, listen_to_health_report_source).await
}

async fn listen_to_health_report_source(
    context: ContextRef,
    source: ServiceUnitDefinition,
) -> anyhow::Result<()> {
    let source_id = source.get_service_bundle_identity();
    let source_vm = source_id.get_fqin().get_sdv_vm_name();
    let untyped_stream = SubscriberStream::create(
        context,
        &source.get_unit_identifier(),
        Duration::ZERO,
        SubscriberOptions { fetch_last_message: true },
    )
    .await?;
    let mut typed_message_stream = sdv::mw::observe::<VmHealth>(Box::pin(untyped_stream));

    while let Some(Ok(reports)) = typed_message_stream.next().await {
        event!(Level::INFO, "{}:HM_PERF_TEST, DT_SUBSCRIBER: Received health report", &source_vm);
        let display_status = if reports
            .iter()
            .all(|r| r.all_monitored_service_bundles_healthy && r.all_service_bundles_alive)
        {
            "HEALTHY"
        } else {
            "UNHEALTHY"
        };
        info!(
            "Received VM health report of vm: {}, report = {:?}, (TEST RELEVANT LOG) VM is {}",
            &source_vm, reports, display_status
        );
    }
    error!("Report consuming loop unexpectedly exited (TEST RELEVANT LOG) VM is UNHEALTHY");
    Err(anyhow!("Report consuming loop unexpectedly exited"))
}

async fn discover_health_reporters_and_launch_listeners<Fut>(
    context: ContextRef,
    listener_factory: impl Fn(ContextRef, ServiceUnitDefinition) -> Fut + std::marker::Send + 'static,
) -> anyhow::Result<()>
where
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    let sd = ServiceDiscoveryManager::new(context);
    let mut pub_events_stream = sd
        .subscribe_service_unit_change_by_type(
            &UnitType::builder()
                .set_service_bundle_name(VmHealth::SERVICE_BUNDLE)
                .set_sdv_package_name(VmHealth::PACKAGE)
                .set_unit_type_name(VmHealth::TYPENAME)
                .build()?,
        )
        .await?;

    let listeners = Arc::new(Mutex::new(JoinSet::new()));
    let discovery_task = {
        let listeners = Arc::clone(&listeners);
        tokio::spawn(async move {
            loop {
                let event = pub_events_stream.next().await;
                info!("Health report unit type event: {event:?}");
                if let Some(Registered(sud)) = event {
                    listeners
                        .lock()
                        .unwrap_or_else(|poison_warn| poison_warn.into_inner())
                        .spawn(listener_factory(context, sud));
                }
            }
        })
    };
    tokio::select! {
        r = discovery_task => r?,
        r = async move{
            // check for unexpected listener completions while allowing new listener spawning
            loop{
                if let Poll::Ready(Some(result)) = listeners.lock().unwrap_or_else(
                    |poison_warn| poison_warn.into_inner()
                ).poll_join_next(&mut Context::from_waker(Waker::noop())){
                    break result?;
                }
                sleep(Duration::from_millis(500)).await;
            }
        } => r
    }
}
