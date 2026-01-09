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

use log::error;
use rustutils::android::system_properties::Result;
use std::time::Duration;

const DEFAULT: Duration = Duration::from_secs(3);
pub fn get_agent_binder_timeout(reader: fn(&str) -> Result<Option<String>>) -> Duration {
    const PROP_NAME: &str = "ro.boot.sdv.health_monitor.agent_startup_timeout_sec";
    match reader(PROP_NAME) {
        Ok(Some(seconds)) => seconds.parse::<u64>().map(Duration::from_secs).unwrap_or_else(|_| {
            error!("Error parsing HM agent binder timeout sys prop. Should be int. Using default: {DEFAULT:?}s");
            DEFAULT
        }),
        _ => {
            error!("Missing HM agent binder timeout sys prop. . Using default: {DEFAULT:?}s");
            DEFAULT
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustutils::android::system_properties::PropertyWatcherError;

    #[test]
    fn test_get_sys_prop() {
        assert!(
            get_agent_binder_timeout(|_| { Ok(Some("1970".to_string())) })
                == Duration::from_secs(1970)
        );
        assert!(get_agent_binder_timeout(|_| { Ok(Some("!@#".to_string())) }) == DEFAULT);
        assert!(get_agent_binder_timeout(|_| { Ok(None) }) == DEFAULT);
        assert!(get_agent_binder_timeout(|_| { Err(PropertyWatcherError::WaitFailed) }) == DEFAULT);
    }
}
