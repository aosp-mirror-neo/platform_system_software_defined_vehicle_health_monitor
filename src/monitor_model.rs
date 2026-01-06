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
use sdv_dump::DumpStateReporter;

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum EntityHealthStatus {
    Healthy,
    Unhealthy,
}

pub trait EntityMonitor: DumpStateReporter {
    fn get_health_status(&self) -> EntityHealthStatus;
    // TODO: to be removed as part of SB crash detection work
    fn get_config(&self) -> &HeartbeatMonitoringConfig;
}

pub type AgentBinderId = String;
pub type AgentName = String;

// TODO: keep struct at least until Orch implements all instance states
#[derive(Clone, Debug, PartialEq)]
pub enum SBRecoveryState {
    Normal,
    Recovering,
    FailedRecovery,
    TerminatedClean,
}

impl EntityMonitor for SBRecoveryState {
    fn get_health_status(&self) -> EntityHealthStatus {
        match self {
            SBRecoveryState::Normal => EntityHealthStatus::Healthy,
            SBRecoveryState::Recovering => EntityHealthStatus::Healthy,
            SBRecoveryState::TerminatedClean => EntityHealthStatus::Healthy,
            SBRecoveryState::FailedRecovery => EntityHealthStatus::Unhealthy,
        }
    }

    fn get_config(&self) -> &HeartbeatMonitoringConfig {
        unimplemented!("This should never be called, it only applies to HB monitors.");
    }
}

impl DumpStateReporter for SBRecoveryState {
    fn dump(
        &self,
        writer: &mut dyn std::io::Write,
        _args: &[&std::ffi::CStr],
    ) -> Result<(), std::io::Error> {
        writeln!(writer, "{self:?}")
    }
}

#[cfg(test)]
pub mod test_utils {
    use std::fmt::Display;

    use crate::{
        hb_config::HeartbeatMonitoringConfig,
        monitor_model::{EntityHealthStatus, EntityMonitor},
    };

    #[derive(Copy, Clone)]
    pub struct TestMonitor {
        pub healthy: EntityHealthStatus,
        pub config: HeartbeatMonitoringConfig,
    }

    impl EntityMonitor for TestMonitor {
        fn get_health_status(&self) -> super::EntityHealthStatus {
            self.healthy
        }

        fn get_config(&self) -> &HeartbeatMonitoringConfig {
            &self.config
        }
    }

    impl Display for TestMonitor {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            writeln!(f, "TEST MONITOR")?;
            writeln!(f, "Healthy: {:?}", self.healthy)?;
            writeln!(f, "Config: {:?}", self.config)
        }
    }

    impl Default for TestMonitor {
        fn default() -> Self {
            Self { healthy: EntityHealthStatus::Healthy, config: Default::default() }
        }
    }

    mod test {
        use crate::{
            hb_config::HeartbeatMonitoringConfig,
            monitor_model::{test_utils::TestMonitor, EntityHealthStatus, EntityMonitor},
        };

        #[test]
        fn test_monitor_default_reports_expected() {
            let monitor = TestMonitor::default();
            assert!(
                monitor.get_health_status() == EntityHealthStatus::Healthy,
                "Test monitor is not healthy!"
            );
            assert!(
                monitor.config == HeartbeatMonitoringConfig::default(),
                "Test monitor is not well configured!"
            );
        }
    }
}
