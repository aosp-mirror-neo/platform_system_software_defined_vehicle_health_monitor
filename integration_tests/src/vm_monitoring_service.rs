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

use std::sync::Arc;

use com_android_sdv_health_health_monitor_service_rpc::Client as HealthMonitorClient;
use health_api::health_topic::VmHealth;
use sdv_comms::{id::ServiceFqin, ContextRef};
use sdv_mw_rs_com_android_sdv_sample_oem_health_monitoring_vm_hm_bundle::{
    subscriber::Variant, VmHMBundle,
};
use sdv_mw_rs_comms::{ClientOptions, RpcClient, SubscribeOptions, Subscriber, UnitName};
use sdv_mw_rs_sdv_comms::SdvComms;

pub struct VmMonitoringService {
    pub dt_subscriber: Subscriber<VmHealth>,
    pub hm_client: HealthMonitorClient,
}

impl VmMonitoringService {
    pub async fn new() -> Self {
        let mut bundle: VmHMBundle = Self::new_service_bundle().await;
        let dt_subscriber: Subscriber<VmHealth> = bundle
            .create_subscriber(Variant::<VmHealth>::UNIQUE, &SubscribeOptions::default())
            .await
            .expect("Failed to create DT subscriber");
        let hm_client: HealthMonitorClient = bundle
            .create_rpc_client::<HealthMonitorClient>(
                UnitName::builder()
                    .package_name("com.android.sdv.health")
                    .bundle_name("HealthMonitorServiceBundle")
                    .service_unit_name(HealthMonitorClient::DEFAULT_UNIT_NAME)
                    .build()
                    .unwrap(),
                ClientOptions::default(),
            )
            .await
            .expect("Failed to create health monitor client");

        Self { dt_subscriber, hm_client }
    }

    async fn new_service_bundle() -> VmHMBundle {
        let fqin = ServiceFqin::builder()
            .sdv_vm_name("vm")
            .sdv_package_name("com.android.sdv.sample.oem.health.monitoring")
            .service_bundle_name("VmHMBundle")
            .service_instance_name("instance1")
            .build()
            .unwrap();
        let comms = Arc::new(SdvComms { context: ContextRef::create(fqin) });
        VmHMBundle::new(comms).await.unwrap()
    }
}
