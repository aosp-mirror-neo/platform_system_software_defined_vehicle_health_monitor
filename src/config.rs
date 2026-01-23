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

//! Implementation for Health Monitor Config.
//!  - parsing of textproto config

use anyhow::anyhow;
use health_api::health_monitor_config::{BinderServiceAgent, HealthMonitorConfiguration};
use log::error;
use log::info;
use protobuf::text_format;
use rustutils::android::system_properties::Result;
use std::fmt::{Display, Formatter};

const DEFAULT_HM_PERIOD: u64 = 1000; // 1000ms = 1s
const HM_CONFIG_PATH_ENV_VAR: &str = "persist.sdv.health_monitor.config_path";

/// Health Monitor Config
#[derive(Debug, PartialEq)]
pub struct Config {
    /// Periodicity of publising a VM health report in ms.
    pub period_ms: u64,
    pub monitored_binders: Vec<BinderServiceAgent>,
}

impl Config {
    pub fn get_from_sys_prop(reader: fn(&str) -> Result<Option<String>>) -> Self {
        match reader(HM_CONFIG_PATH_ENV_VAR) {
            Ok(Some(filepath)) => Config::from_filepath(&filepath).unwrap_or_else(|err| {
                error!("Error parsing textproto or invalid config: {err}. Using default");
                Config::default()
            }),
            _ => {
                error!("Missing or empty HM agent binder timeout sys prop. Using default");
                Config::default()
            }
        }
    }

    /// Configures health monitor using a proto-based file
    ///
    /// Loads the textproto file from path, and parses it. Returns default config if loading or parsing fails.
    ///
    /// # Arguments
    ///
    /// * `config_file_path` - A string slice that holds the path of the config file.
    pub fn from_filepath(config_file_path: &str) -> anyhow::Result<Self> {
        let config_textproto = std::fs::read_to_string(config_file_path)?;

        let config = text_format::parse_from_str::<HealthMonitorConfiguration>(&config_textproto)
            .map_err(|e| anyhow!(e.to_string()))?;
        if config.period_ms < 1 {
            return Err(anyhow!("Period for publishing VM health report should be a positive integer number, provided {} ms", config.period_ms));
        }
        info!(
            "Successfully parsed health monitor config proto. Reporting timeout is {:?} ms",
            config.period_ms
        );
        Ok(Config { period_ms: config.period_ms, monitored_binders: config.monitored_agent })
    }
}

impl Display for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        writeln!(f, "VM health report period (ms): {}", self.period_ms)?;
        writeln!(f, "Crash monitoring configuration:\n{:#?}", self.monitored_binders)?;
        Ok(())
    }
}

impl Default for Config {
    /// Creates a new health monitor configuration
    fn default() -> Self {
        Self { period_ms: DEFAULT_HM_PERIOD, monitored_binders: vec![] }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use googletest::prelude::*;
    use rustutils::android::system_properties::PropertyWatcherError;

    #[test]
    fn test_config_not_found() {
        let res = Config::from_filepath("missing_config.textproto");
        assert!(res.is_err());
    }

    #[test]
    fn test_config_invalid() {
        let res = Config::from_filepath("src/test_configs/config_invalid.textproto");
        assert!(res.is_err());
    }

    #[test]
    fn test_errors_in_processing_textproto_lead_to_default_config() {
        let config = Config::get_from_sys_prop(|_| {
            Ok(Some("src/test_configs/config_invalid.textproto".to_string()))
        });
        assert_eq!(config, Config::default());
    }
    #[test]
    fn test_errors_in_sys_prop_reading_lead_to_default_config() {
        let config = Config::get_from_sys_prop(|_| Ok(None));
        assert_eq!(config, Config::default());

        let config = Config::get_from_sys_prop(|_| Err(PropertyWatcherError::WaitFailed));
        assert_eq!(config, Config::default());
    }

    #[test]
    fn test_parse_periods_ms() {
        let config = Config::from_filepath("src/test_configs/config.textproto");
        if let Ok(config) = config {
            assert_eq!(config.period_ms, 10);
        } else {
            panic!("Did not correctly parse simple config textproto");
        }
    }

    #[test]
    fn test_config_invalid_periods_ms() {
        let res = Config::from_filepath("src/test_configs/config_invalid_zeroms.textproto");
        assert_that!(res, err(anything()));
    }

    #[test]
    fn test_agent_crash_monitoring_config_correctly_parsed() {
        let config =
            Config::from_filepath("src/test_configs/config_with_crash_monitoring.textproto")
                .unwrap();
        assert!(config.monitored_binders[0].agent_name == "foo");
        assert!(config.monitored_binders[0].binder_interface_name == "bar");
        assert!(config.monitored_binders[1].agent_name == "foo2");
        assert!(config.monitored_binders[1].binder_interface_name == "bar2");
    }
}
