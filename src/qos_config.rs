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
#![allow(dead_code)]

use health_api::health_config::QosMonitoringConfiguration;
use sdv_status_rs::{SdvStatus, SdvStatusCode};
use std::time::Duration;

#[derive(Clone)]
pub struct QosConfig {
    pub latency_threshold: Option<Duration>,
    // Monitors the frequency of the communication. Allowed max period between two heartbeats
    pub frequency_threshold: Option<Duration>,
}

impl TryFrom<QosMonitoringConfiguration> for QosConfig {
    type Error = SdvStatus;

    fn try_from(value: QosMonitoringConfiguration) -> Result<Self, Self::Error> {
        let latency_threshold = value.qos_latency_threshold_ms.map(Duration::from_millis);
        let frequency_threshold = value.qos_period_threshold_ms.map(Duration::from_millis);

        if latency_threshold.is_none() && frequency_threshold.is_none() {
            return Err(SdvStatus::new(SdvStatusCode::InvalidArgument));
        }
        Ok(Self { latency_threshold, frequency_threshold })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_qos_config_try_from_valid() {
        let mut config_proto = QosMonitoringConfiguration::new();
        config_proto.qos_latency_threshold_ms = Some(100);
        config_proto.qos_period_threshold_ms = Some(200);

        let config = QosConfig::try_from(config_proto).unwrap();
        assert_eq!(config.latency_threshold.unwrap(), Duration::from_millis(100));
        assert_eq!(config.frequency_threshold.unwrap(), Duration::from_millis(200));
    }

    #[test]
    fn test_qos_config_try_from_some_missing_fields_ok() {
        let mut config_proto = QosMonitoringConfiguration::new();
        config_proto.qos_latency_threshold_ms = Some(100);
        // missing qos_period_threshold_ms

        let config = QosConfig::try_from(config_proto).unwrap();
        assert_eq!(config.latency_threshold.unwrap(), Duration::from_millis(100));
        assert_eq!(config.frequency_threshold, None);
    }

    #[test]
    fn test_qos_config_try_from_fails_when_both_missing_fields() {
        let config_proto = QosMonitoringConfiguration::new();

        let result = QosConfig::try_from(config_proto);
        assert!(result.is_err());
    }
}
