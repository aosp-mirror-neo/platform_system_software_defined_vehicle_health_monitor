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

use crate::hb_config::HeartbeatMonitoringConfig;
use crate::qos_config::QosConfig;
use binder::Strong;
use google_sdv_service_bundles_registry::aidl::google::sdv::service_bundles_registry::IRegistry::{
    BpRegistry, IRegistry,
};
use google_sdv_service_bundles_registry::aidl::google::sdv::service_bundles_registry::ServiceBundleMetadata::ServiceBundleMetadata;
use health_api::health_config::ServiceBundleHealthConfiguration;
use log::{error, info, warn};
use protobuf::text_format::parse_from_str;
use tap::Pipe;
use sdv_comms::id::ServiceFqin;
use sdv_status_rs::{SdvResult, SdvStatus, SdvStatusCode};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;

#[derive(Debug)]
pub struct InstanceConfig {
    pub hb_config: Option<HeartbeatMonitoringConfig>,
    pub qos_config: HashMap<String, QosConfig>, // key: topic, value: QosConfig
}

pub struct ImplicitMonitoringConfigRepo {
    data: OnceCell<HashMap<ServiceFqin, InstanceConfig>>,
    vm_name_provider: Arc<dyn Fn() -> String + Send + Sync>,
}

impl ImplicitMonitoringConfigRepo {
    pub fn new(vm_name_provider: Arc<dyn Fn() -> String + Send + Sync>) -> Self {
        ImplicitMonitoringConfigRepo { data: OnceCell::default(), vm_name_provider }
    }

    async fn get_sb_instance_config(
        &self,
        fqin: &ServiceFqin,
    ) -> SdvResult<Option<&InstanceConfig>> {
        let vm_name_provider = Arc::clone(&self.vm_name_provider);
        self.data
            .get_or_try_init(|| async {
                tokio::task::spawn_blocking(move || {
                    let vm_name = vm_name_provider();
                    retrieve_implicit_health_config_from_sbr(get_bundle_metadata, &vm_name)
                })
                .await
                .map_err(|e| {
                    SdvStatus::with_message(SdvStatusCode::Internal, format!("JoinError: {e}"))
                })
            })
            .await?
            .get(fqin)
            .pipe(Ok)
    }

    pub async fn get_implicit_hb_config(
        &self,
        fqin: &ServiceFqin,
    ) -> SdvResult<Option<HeartbeatMonitoringConfig>> {
        self.get_sb_instance_config(fqin).await?.and_then(|c| c.hb_config).pipe(Ok)
    }

    pub async fn get_qos_config(
        &self,
        fqin: &ServiceFqin,
    ) -> SdvResult<Option<&HashMap<String, QosConfig>>> {
        self.get_sb_instance_config(fqin).await?.map(|c| &c.qos_config).pipe(Ok)
    }
}

/// NOTE: might block thread. Use separate run_blocking
fn retrieve_implicit_health_config_from_sbr(
    get_bundle_metadata: impl FnOnce() -> Vec<ServiceBundleMetadata>,
    vm_name: &str,
) -> HashMap<ServiceFqin, InstanceConfig> {
    get_bundle_metadata()
        .into_iter()
        .filter_map(|metadata| {
            if let Some(path) = metadata.healthConfigPath {
                Some((metadata.packageName, metadata.name, path))
            } else {
                None
            }
        })
        .inspect(|path: &(String, String, String)| info!("Health config config path: {path:#?}"))
        .flat_map(|(package, name, path)| {
            parse_bundle_health_config(&path)
                .unwrap_or_else(|e| {
                    error!("Error {:?} parsing health config from path: {:?}", e, path);
                    HashMap::default()
                })
                .into_iter()
                .map(move |(instance, config)| {
                    (
                        ServiceFqin::builder()
                            .sdv_vm_name(vm_name)
                            .sdv_package_name(&package)
                            .service_bundle_name(&name)
                            .service_instance_name(&instance)
                            .build(),
                        config,
                    )
                })
        })
        .filter_map(|(key, value)| {
            if let Ok(fqin) = key {
                Some((fqin, value))
            } else {
                error!("Error constructing fqin");
                None
            }
        })
        .collect()
}

fn parse_bundle_health_config(path: &str) -> SdvResult<HashMap<String, InstanceConfig>> {
    let mut file = File::open(path).map_err(|e| {
        SdvStatus::with_message(SdvStatusCode::Internal, format!("File open error: {e}"))
    })?;
    let mut textproto = String::new();
    file.read_to_string(&mut textproto).map_err(|e| {
        SdvStatus::with_message(SdvStatusCode::Internal, format!("File read error: {e}"))
    })?;

    let map = parse_from_str::<ServiceBundleHealthConfiguration>(&textproto)
        .map_err(|e| {
            SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                format!("Proto parse error: {e}"),
            )
        })?
        .instance_config
        .into_iter()
        .map(|(fqin, conf)| {
            (
                fqin,
                conf.health_config
                    .into_option()
                    .and_then(|hc| HeartbeatMonitoringConfig::try_from(hc).ok()),
                conf.qos_config
                    .into_iter()
                    .filter_map(|(k, v)| (k, TryInto::<QosConfig>::try_into(v).ok()?).pipe(Some))
                    .collect(),
            )
        })
        .map(|(k, hb_config, qos_config)| (k, InstanceConfig { hb_config, qos_config }))
        .collect::<HashMap<_, _>>();

    if !map.is_empty() {
        Ok(map)
    } else {
        Err(SdvStatus::new(SdvStatusCode::NotFound))
    }
}

const SBR_RETRY_COOLDOWN: Duration = Duration::from_secs(3);

/// Gets metadata of all service bundles once, dropping binder connection afterwards
/// NOTE: blocking function.  Call from appropriate thread
fn get_bundle_metadata() -> Vec<ServiceBundleMetadata> {
    let sbr_binder: Strong<dyn IRegistry> = retry_forever_until_ok(
        || {
            let descriptor = <BpRegistry as IRegistry>::get_descriptor().to_owned() + "/default";
            Ok(binder::wait_for_interface::<dyn IRegistry>(&descriptor)?)
        },
        SBR_RETRY_COOLDOWN,
        "Error fetching SBR binder interface",
    );
    retry_forever_until_ok(
        move || Ok(sbr_binder.getAvailableServiceBundlesMetadata()?),
        SBR_RETRY_COOLDOWN,
        "Error retrieving SB manifests from SBR binder",
    )
}

fn retry_forever_until_ok<T>(
    mut logic: impl FnMut() -> anyhow::Result<T>,
    cooldown: Duration,
    err_explanation: &str,
) -> T {
    loop {
        let res: anyhow::Result<T> = logic();
        match res {
            Ok(res) => {
                return res;
            }
            Err(e) => {
                warn!("{}: Failure: {}. Retrying in {:?}", e, err_explanation, cooldown);
                std::thread::sleep(cooldown);
            }
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use anyhow::anyhow;

    pub mod utils {
        use super::*;

        impl ImplicitMonitoringConfigRepo {
            pub fn new_for_testing(inner: HashMap<ServiceFqin, InstanceConfig>) -> Self {
                Self {
                    data: OnceCell::new_with(Some(inner)),
                    vm_name_provider: Arc::new(|| "testvm".to_string()),
                }
            }
        }
    }

    #[tokio::test]
    async fn test_retrieve_config_success_when_config_exists() {
        let fqin = ServiceFqin::builder()
            .sdv_vm_name("testvm")
            .sdv_package_name("my.package")
            .service_bundle_name("CoolBundle")
            .service_instance_name("instance1")
            .build()
            .unwrap();

        let db = ImplicitMonitoringConfigRepo::new_for_testing(HashMap::from([(
            fqin.clone(),
            InstanceConfig {
                hb_config: Some(HeartbeatMonitoringConfig::default()),
                qos_config: HashMap::default(),
            },
        )]));

        assert!(db.get_sb_instance_config(&fqin).await.unwrap().is_some())
    }

    #[tokio::test]
    async fn test_retrieve_config_error_when_config_missing() {
        let fqin = ServiceFqin::builder()
            .sdv_vm_name("testvm")
            .sdv_package_name("my.package")
            .service_bundle_name("CoolBundle")
            .service_instance_name("instance1")
            .build()
            .unwrap();

        let db = ImplicitMonitoringConfigRepo::new_for_testing(HashMap::default());

        assert!(db.get_sb_instance_config(&fqin).await.unwrap().is_none())
    }

    #[test]
    fn test_parse_health_config_from_path_when_file_valid() {
        let res = parse_bundle_health_config(
            "src/test_configs/implicit_per_bundle_health_configs/valid.textproto",
        )
        .expect("Expecting parsing successful, as textproto is valid");

        assert!(res.contains_key("instance1"));
        assert!(res.contains_key("instance2"));
    }

    #[test]
    fn test_parse_health_config_from_path_when_file_valid_qos_only() {
        let res = parse_bundle_health_config(
            "src/test_configs/implicit_per_bundle_health_configs/valid_qos_only.textproto",
        )
        .expect("Expecting parsing successful, as textproto is valid");

        assert!(res.contains_key("instance1"));
        let qos_config = &res.get("instance1").unwrap().qos_config;
        assert_eq!(qos_config.len(), 2);

        let qos_config_first_unit = qos_config.get("my-qos-topic-name-1").unwrap();
        let qos_config_second_unit = qos_config.get("my-qos-topic-name-2").unwrap();

        assert!(qos_config_first_unit.frequency_threshold.is_some());
        assert!(qos_config_first_unit.latency_threshold.is_none());

        assert!(qos_config_second_unit.frequency_threshold.is_some());
        assert!(qos_config_second_unit.latency_threshold.is_some());
    }

    #[test]
    fn test_parse_health_config_from_path_when_file_invalid() {
        let res = parse_bundle_health_config(
            "src/test_configs/implicit_per_bundle_health_configs/invalid.textproto",
        );

        assert!(res.is_err(), "parsing an invalid textproto file did not return an error!");
    }

    #[test]
    fn test_parse_health_config_filters_invalid_qos_entries() {
        let res = parse_bundle_health_config(
            "src/test_configs/implicit_per_bundle_health_configs/qos_with_invalid.textproto",
        )
        .expect("Expecting parsing successful");

        assert!(res.contains_key("instance1"));
        let qos_config = &res.get("instance1").unwrap().qos_config;

        // "valid-qos" should be present, "invalid-qos-empty" should be filtered out
        assert_eq!(qos_config.len(), 1);
        assert!(qos_config.contains_key("valid-qos"));
        assert!(!qos_config.contains_key("invalid-qos-empty"));
    }

    #[test]
    fn test_parse_health_config_from_path_when_path_invalid() {
        let res = parse_bundle_health_config("invalid/path");
        assert!(res.is_err(), "parsing from an invalid path did not return an error!");
    }

    #[test]
    fn test_metadata_structure_maps_to_map_of_health_configs() {
        // based on retrieved metadata from sbr, tests that the expected "config db" is constructed
        // tests input, sbr, contains two bundles that point to a valid text proto, each with two
        // instances configured for health monitoring.

        let my_package = "my.package";
        let my_other_package = "my.other.package";

        let my_bundle = "MyBundle";
        let my_other_bundle = "MyOtherBundle";
        let bundle_with_invalid_config = "BundleWithInvalidConfig";

        let fqin1 = ServiceFqin::builder()
            .sdv_vm_name("testvm")
            .sdv_package_name(my_package)
            .service_bundle_name(my_bundle)
            .service_instance_name("instance1")
            .build()
            .unwrap();

        let fqin2 = ServiceFqin::builder()
            .sdv_vm_name("testvm")
            .sdv_package_name(my_package)
            .service_bundle_name(my_bundle)
            .service_instance_name("instance2")
            .build()
            .unwrap();

        let fqin3 = ServiceFqin::builder()
            .sdv_vm_name("testvm")
            .sdv_package_name(my_package)
            .service_bundle_name(my_other_bundle)
            .service_instance_name("instance1")
            .build()
            .unwrap();

        let fqin4 = ServiceFqin::builder()
            .sdv_vm_name("testvm")
            .sdv_package_name(my_package)
            .service_bundle_name(my_other_bundle)
            .service_instance_name("instance2")
            .build()
            .unwrap();

        let fqin5 = ServiceFqin::builder()
            .sdv_vm_name("testvm")
            .sdv_package_name(my_package)
            .service_bundle_name(bundle_with_invalid_config)
            .service_instance_name("any")
            .build()
            .unwrap();

        // SETUP
        let metadata = Vec::from([
            ServiceBundleMetadata {
                packageName: my_package.to_string(),
                name: my_bundle.to_string(),
                healthConfigPath:
                    "src/test_configs/implicit_per_bundle_health_configs/valid.textproto"
                        .to_string()
                        .into(),
                ..ServiceBundleMetadata::default()
            },
            ServiceBundleMetadata {
                packageName: my_package.to_string(),
                name: my_other_bundle.to_string(),
                healthConfigPath:
                    "src/test_configs/implicit_per_bundle_health_configs/valid.textproto"
                        .to_string()
                        .into(),
                ..ServiceBundleMetadata::default()
            },
            ServiceBundleMetadata::default(),
            ServiceBundleMetadata {
                packageName: my_other_package.to_string(),
                name: bundle_with_invalid_config.to_string(),
                healthConfigPath:
                    "src/test_configs/implicit_per_bundle_health_configs/invalid.textproto"
                        .to_string()
                        .into(),
                ..ServiceBundleMetadata::default()
            },
        ]);

        // ACT
        let config_db = retrieve_implicit_health_config_from_sbr(move || metadata, "testvm");

        // ASSERT
        assert!(config_db.contains_key(&fqin1));
        assert!(config_db.contains_key(&fqin2));
        assert!(config_db.contains_key(&fqin3));
        assert!(config_db.contains_key(&fqin4));
        assert!(!config_db.contains_key(&fqin5));
    }

    #[test]
    fn test_retry_forever_until_ok() {
        const MAX_TIMES: i32 = 3;
        let mut times_called = 0;
        let logic = || {
            times_called += 1;
            if times_called == MAX_TIMES {
                Ok(())
            } else {
                Err(anyhow!("err"))
            }
        };

        retry_forever_until_ok(
            logic,
            Duration::from_millis(1),
            "logic failed. This is expected, 3 times",
        );

        assert!(times_called == MAX_TIMES, "retry_forever_until_ok is broken");
    }
}
