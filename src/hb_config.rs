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

use health_api::health_config::HealthConfiguration;

#[derive(Clone, PartialEq, Debug, Default, Copy)]
pub struct HeartbeatMonitoringConfig {
    // The time between the service start and its first heartbeat.
    pub initial_delay_ms: u64,

    // Period of reporting a heartbeat which corresponds to the periodicity of executing a periodic
    // task by the SDV service.
    pub period_ms: u64,

    // The number of periods to be missed before the Health Monitor should consider the
    // service as unhealthy.
    // A service is marked as unhealthy if there was no heartbeat in the last:
    // (period_ms * num_periods + task_duration_ms).
    pub num_periods: u64,

    // Duration of the task the SDV service executes.
    pub task_duration_ms: u64,
}

impl HeartbeatMonitoringConfig {
    /// Validates the provided `HeartbeatMonitoringConfig` to ensure all parameters are within acceptable ranges.
    ///
    /// # Arguments
    ///
    /// * `health_config` - The `HeartbeatMonitoringConfig` to validate.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the configuration is valid.
    /// * `Err(String)` if any parameter is invalid, with a descriptive error message.
    fn validate_health_config(config: &HeartbeatMonitoringConfig) -> Result<(), String> {
        if config.period_ms == 0 {
            return Err("period_ms must be greater than 0 ms".to_string());
        }

        if config.num_periods == 0 {
            return Err("num_periods must be greater than 0".to_string());
        }
        Ok(())
    }
}

impl TryFrom<HealthConfiguration> for HeartbeatMonitoringConfig {
    type Error = String;
    fn try_from(value: HealthConfiguration) -> Result<Self, Self::Error> {
        let config = Self {
            initial_delay_ms: value.initial_delay_ms.unwrap_or_default(),
            period_ms: value.period_ms.unwrap_or_default(),
            num_periods: value.num_periods.unwrap_or_default(),
            task_duration_ms: value.task_duration_ms.unwrap_or_default(),
        };
        HeartbeatMonitoringConfig::validate_health_config(&config).map(|_| config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from_valid_health_configuration() {
        let health_config = HealthConfiguration {
            initial_delay_ms: Some(100),
            period_ms: Some(200),
            num_periods: Some(3),
            task_duration_ms: Some(40),
            ..Default::default()
        };

        let service_health_config = HeartbeatMonitoringConfig::try_from(health_config);

        assert!(service_health_config.is_ok());

        let service_health_config = service_health_config.unwrap();
        assert_eq!(service_health_config.initial_delay_ms, 100);
        assert_eq!(service_health_config.period_ms, 200);
        assert_eq!(service_health_config.num_periods, 3);
        assert_eq!(service_health_config.task_duration_ms, 40);
    }

    #[test]
    fn test_try_from_health_configuration_with_invalid_period_ms() {
        let health_config = HealthConfiguration {
            initial_delay_ms: Some(100),
            period_ms: Some(0), // Invalid: period_ms must be greater than 0
            num_periods: Some(3),
            task_duration_ms: Some(40),
            ..Default::default()
        };

        let service_health_config = HeartbeatMonitoringConfig::try_from(health_config);

        assert!(service_health_config.is_err());
        assert_eq!(service_health_config.unwrap_err(), "period_ms must be greater than 0 ms");
    }

    #[test]
    fn test_try_from_health_configuration_with_invalid_num_periods() {
        let health_config = HealthConfiguration {
            initial_delay_ms: Some(100),
            period_ms: Some(200),
            num_periods: Some(0), // Invalid: num_periods must be greater than 0
            task_duration_ms: Some(40),
            ..Default::default()
        };

        let service_health_config = HeartbeatMonitoringConfig::try_from(health_config);

        assert!(service_health_config.is_err());
        assert_eq!(service_health_config.unwrap_err(), "num_periods must be greater than 0");
    }
}
