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

use sdv_dump::DumpStateReporter;

use crate::hb_config::HeartbeatMonitoringConfig;
use crate::EntityHealthStatus;
use crate::EntityMonitor;

/// Service Bundle Instance Recovery State
#[derive(Clone, Debug, PartialEq, Copy)]
pub enum SBRecoveryState {
    Normal,
    Recovering,
    FailedRecovery,
}

#[allow(dead_code)] // TODO once aidl gets updated
/// Service Bundle Instance Lifecycle State
#[derive(Clone, Debug, PartialEq, Copy)]
pub enum SBLifecycleState {
    Started,
    Created,
    Destroyed,
}

#[derive(Debug, Clone, Copy)]
pub struct SBOrchestrationState {
    pub(crate) recovery_state: SBRecoveryState,
    pub(crate) lifecycle_state: SBLifecycleState,
}

/// Last orchestration event contains complete data required for recovery monitoring
pub type SBRecoveryMonitor = SBOrchestrationState;

impl EntityMonitor for SBRecoveryMonitor {
    fn get_health_status(&self) -> EntityHealthStatus {
        match self.recovery_state {
            SBRecoveryState::Normal => EntityHealthStatus::Healthy,
            SBRecoveryState::Recovering => EntityHealthStatus::Healthy,
            SBRecoveryState::FailedRecovery => EntityHealthStatus::Unhealthy,
        }
    }

    fn get_config(&self) -> &HeartbeatMonitoringConfig {
        unimplemented!("This should never be called, it only applies to HB monitors.");
    }
}

impl DumpStateReporter for SBRecoveryMonitor {
    fn dump(
        &self,
        writer: &mut dyn std::io::Write,
        _args: &[&std::ffi::CStr],
    ) -> Result<(), std::io::Error> {
        writeln!(writer, "Recovery State: {:?}", self.recovery_state)?;
        writeln!(writer, "Lifecycle State: {:?}", self.lifecycle_state)?;
        writeln!(writer, "Health Status: {:?}", self.get_health_status())
    }
}
