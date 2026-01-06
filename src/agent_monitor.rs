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

use crate::monitor_model::AgentBinderId;
use crate::monitor_model::AgentName;
use crate::monitor_model::EntityHealthStatus;
use crate::monitor_model::EntityMonitor;
use crate::power_state::PowerState;
use crate::power_state::{PowerStateInfo, PowerStateReceiverFactory};
use crate::read_sys_prop::get_agent_binder_timeout;
use binder::IBinder;
use binder::SpIBinder;
use health_api::health_monitor_config::BinderServiceAgent;
use log::{error, warn};
use sdv_dump::DumpStateReporter;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub fn create_agent_monitor_entries(
    config: Vec<BinderServiceAgent>,
    power_state_rx_factory: &dyn PowerStateReceiverFactory,
) -> HashMap<AgentName, Box<dyn EntityMonitor>> {
    let binder_timeout = get_agent_binder_timeout(rustutils::android::system_properties::read);
    config
        .into_iter()
        .map(|monitor_config| {
            let binder = monitor_config.binder_interface_name;
            (
                monitor_config.agent_name,
                Box::new(AgentHealthMonitor::<BinderInteractionImpl>::new(
                    binder,
                    binder_timeout,
                    power_state_rx_factory.create_receiver(),
                )) as Box<dyn EntityMonitor>,
            )
        })
        .collect()
}

pub struct AgentHealthMonitor<T: BinderInteraction> {
    agent_healthy: Arc<AtomicBool>,
    binder: Arc<AgentBinderId>,
    init_link_to_death_task: JoinHandle<anyhow::Result<(T::BinderObj, T::DeathCallback)>>,
}

impl<T: BinderInteraction> AgentHealthMonitor<T> {
    const DUMMY_CONFIG: crate::hb_config::HeartbeatMonitoringConfig =
        crate::hb_config::HeartbeatMonitoringConfig {
            initial_delay_ms: 0,
            period_ms: 0,
            num_periods: 0,
            task_duration_ms: 0,
        };

    fn new(
        binder: AgentBinderId,
        link_timeout: Duration,
        power_state_rx: Receiver<PowerStateInfo>,
    ) -> Self {
        let agent_healthy = Arc::new(AtomicBool::new(true));
        let binder = Arc::new(binder);
        let init_link_to_death_task = {
            let binder = Arc::clone(&binder);
            let agent_healthy = Arc::clone(&agent_healthy);
            tokio::task::spawn(async move {
                Self::link_monitor_to_binder_death_event(
                    agent_healthy,
                    binder,
                    link_timeout,
                    power_state_rx,
                )
                .await
            })
        };
        AgentHealthMonitor { agent_healthy, binder, init_link_to_death_task }
    }

    async fn link_monitor_to_binder_death_event(
        agents_health_state_holder: Arc<AtomicBool>,
        binder_interface: Arc<String>,
        link_timeout: Duration,
        power_state_rx: watch::Receiver<PowerStateInfo>,
    ) -> anyhow::Result<(T::BinderObj, T::DeathCallback)> {
        let agents_healthy = Arc::clone(&agents_health_state_holder);
        let binder = timeout(
            link_timeout,
            tokio::task::spawn_blocking({
                let binder_interface = Arc::clone(&binder_interface);
                move || T::wait_for_service(binder_interface.as_ref())
            }),
        )
        .await
        .inspect_err(|e| {
            error!("Binder interface not exposed in time {e}. {binder_interface} is unhealthy");
            agents_healthy.store(false, Ordering::SeqCst);
        })?
        .ok()
        .flatten();

        let Some(mut binder) = binder else {
            agents_healthy.store(false, Ordering::SeqCst);
            return Err(anyhow::anyhow!("Error waiting for service {}", binder_interface));
        };

        let mut death_recipient = T::create_death_callback({
            move || {
                // If we are shutting down, we don't need to report agents that are getting terminated.
                if !matches!(power_state_rx.borrow().stage, PowerState::ShuttingDown) {
                    warn!("Critical agent has died");
                    agents_health_state_holder.store(false, Ordering::SeqCst);
                }
            }
        });
        T::link_to_death(&mut binder, &mut death_recipient).inspect_err(|e| {
            error!("Error linking to death: {e:?}");
            agents_healthy.store(false, Ordering::SeqCst);
        })?;
        Ok((binder, death_recipient))
    }
}

impl<T: BinderInteraction> Drop for AgentHealthMonitor<T> {
    fn drop(&mut self) {
        self.init_link_to_death_task.abort()
    }
}

impl<T: BinderInteraction> EntityMonitor for AgentHealthMonitor<T> {
    fn get_config(&self) -> &crate::hb_config::HeartbeatMonitoringConfig {
        error!(
            "AgentHealthMonitor is a special, default monitor. Check logic, this should never be called."
        );
        &Self::DUMMY_CONFIG
    }

    fn get_health_status(&self) -> EntityHealthStatus {
        if self.agent_healthy.load(Ordering::SeqCst) {
            EntityHealthStatus::Healthy
        } else {
            EntityHealthStatus::Unhealthy
        }
    }
}

impl<T: BinderInteraction> DumpStateReporter for AgentHealthMonitor<T> {
    fn dump(
        &self,
        writer: &mut dyn std::io::Write,
        _args: &[&std::ffi::CStr],
    ) -> Result<(), std::io::Error> {
        writeln!(
            writer,
            "linked_binder: {}\n\
            alive: {}\n",
            self.binder,
            self.agent_healthy.load(Ordering::Relaxed),
        )
    }
}
pub trait BinderInteraction: Send + Sync + 'static {
    type DeathCallback: Send + Sync;
    type BinderObj: Send + Sync;

    fn wait_for_service(name: &str) -> Option<Self::BinderObj>;

    fn create_death_callback(callback: impl Fn() + Send + Sync + 'static) -> Self::DeathCallback;

    fn link_to_death(
        binder: &mut Self::BinderObj,
        callback: &mut Self::DeathCallback,
    ) -> Result<(), binder::StatusCode>;
}

pub struct BinderInteractionImpl;
impl BinderInteraction for BinderInteractionImpl {
    type DeathCallback = binder::DeathRecipient;
    type BinderObj = SpIBinder;

    fn wait_for_service(name: &str) -> Option<Self::BinderObj> {
        binder::wait_for_service(name)
    }

    fn create_death_callback(callback: impl Fn() + Send + Sync + 'static) -> Self::DeathCallback {
        binder::DeathRecipient::new(callback)
    }

    fn link_to_death(
        binder: &mut Self::BinderObj,
        callback: &mut Self::DeathCallback,
    ) -> Result<(), binder::StatusCode> {
        binder.link_to_death(callback)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::EntityHealthStatus;
    use std::sync::Mutex;
    use std::thread;
    use std::time::SystemTime;
    pub mod utils {
        use super::*;
        use std::time::Duration;
        use tokio::time::sleep;
        use tokio::time::timeout;

        impl<T: BinderInteraction> AgentHealthMonitor<T> {
            const TIMEOUT: Duration = Duration::from_secs(10);
            const POLL_INTERVAL: Duration = Duration::from_millis(100);

            /// Returns true if condition evaluates to true after some polling
            pub async fn poll_with_timeout(holder: impl Fn() -> bool) -> bool {
                let poll_task = async move || {
                    while !holder() {
                        sleep(Self::POLL_INTERVAL).await;
                    }
                };
                timeout(Self::TIMEOUT, poll_task()).await.is_ok()
            }

            pub async fn poll_with_timeout_for_init_end(&self) -> bool {
                Self::poll_with_timeout(|| self.init_link_to_death_task.is_finished()).await
            }
        }

        #[cfg(test)]
        mod test_util_tests {
            use crate::agent_monitor::{AgentHealthMonitor, BinderInteractionImpl};

            #[tokio::test]
            async fn poll_returns_false_when_condition_does_not_eventually_evaluate_to_true() {
                assert!(
                    !AgentHealthMonitor::<BinderInteractionImpl>::poll_with_timeout(|| false).await
                );
            }
        }
    }

    #[tokio::test]
    async fn monitor_construction_happy_path() {
        let (_, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(SystemTime::now()),
        });

        pub struct MockBinderInteraction;
        impl BinderInteraction for MockBinderInteraction {
            type DeathCallback = ();
            type BinderObj = ();

            fn wait_for_service(_name: &str) -> Option<Self::BinderObj> {
                Some(())
            }

            fn create_death_callback(
                _callback: impl Fn() + Send + Sync + 'static,
            ) -> Self::DeathCallback {
            }

            fn link_to_death(
                _binder: &mut Self::BinderObj,
                _callback: &mut Self::DeathCallback,
            ) -> Result<(), binder::StatusCode> {
                Ok(())
            }
        }
        let monitor = AgentHealthMonitor::<MockBinderInteraction>::new(
            "ignored".to_string(),
            Duration::from_secs(u64::MAX),
            rx,
        );
        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn constructor_no_binder_returned_leads_to_error() {
        let (_, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(SystemTime::now()),
        });

        pub struct MockBinderInteractionNoBinder;
        impl BinderInteraction for MockBinderInteractionNoBinder {
            type DeathCallback = ();
            type BinderObj = ();

            fn wait_for_service(_name: &str) -> Option<Self::BinderObj> {
                None
            }

            fn create_death_callback(
                _callback: impl Fn() + Send + Sync + 'static,
            ) -> Self::DeathCallback {
            }

            fn link_to_death(
                _binder: &mut Self::BinderObj,
                _callback: &mut Self::DeathCallback,
            ) -> Result<(), binder::StatusCode> {
                Ok(())
            }
        }
        let monitor = AgentHealthMonitor::<MockBinderInteractionNoBinder>::new(
            "ignored".to_string(),
            Duration::from_secs(u64::MAX),
            rx,
        );
        assert!(monitor.poll_with_timeout_for_init_end().await);
        assert!(monitor.get_health_status() == EntityHealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn link_to_death_trigger_leads_to_unhealthy() {
        let (_, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(SystemTime::now()),
        });

        static CALLBACK_CAPTURE: Mutex<Option<Box<dyn Fn() + Send + Sync + 'static>>> =
            Mutex::new(None);
        pub struct CallbackCapture;
        impl BinderInteraction for CallbackCapture {
            type DeathCallback = ();
            type BinderObj = ();

            fn wait_for_service(_name: &str) -> Option<Self::BinderObj> {
                Some(())
            }

            fn create_death_callback(
                callback: impl Fn() + Send + Sync + 'static,
            ) -> Self::DeathCallback {
                *CALLBACK_CAPTURE.lock().unwrap() = Some(Box::new(callback));
            }

            fn link_to_death(
                _binder: &mut Self::BinderObj,
                _callback: &mut Self::DeathCallback,
            ) -> Result<(), binder::StatusCode> {
                Ok(())
            }
        }

        let is_healthy = Arc::new(AtomicBool::new(true));
        let result = AgentHealthMonitor::<CallbackCapture>::link_monitor_to_binder_death_event(
            Arc::clone(&is_healthy),
            "irrelevant".to_string().into(),
            Duration::from_secs(u64::MAX),
            rx,
        )
        .await;

        assert!(result.is_ok());
        assert!(is_healthy.load(Ordering::SeqCst));
        if let Some(callback) = CALLBACK_CAPTURE.lock().unwrap().as_ref() {
            callback();
        } else {
            panic!("Death callback was not captured by test");
        }
        assert!(!is_healthy.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn link_to_death_trigger_during_shutdown_is_ignored() {
        let (_, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::ShuttingDown,
            last_resume_time: Some(SystemTime::now()),
        });

        static CALLBACK_CAPTURE: Mutex<Option<Box<dyn Fn() + Send + Sync + 'static>>> =
            Mutex::new(None);
        pub struct CallbackCapture;
        impl BinderInteraction for CallbackCapture {
            type DeathCallback = ();
            type BinderObj = ();

            fn wait_for_service(_name: &str) -> Option<Self::BinderObj> {
                Some(())
            }

            fn create_death_callback(
                callback: impl Fn() + Send + Sync + 'static,
            ) -> Self::DeathCallback {
                *CALLBACK_CAPTURE.lock().unwrap() = Some(Box::new(callback));
            }

            fn link_to_death(
                _binder: &mut Self::BinderObj,
                _callback: &mut Self::DeathCallback,
            ) -> Result<(), binder::StatusCode> {
                Ok(())
            }
        }

        let is_healthy = Arc::new(AtomicBool::new(true));
        let result = AgentHealthMonitor::<CallbackCapture>::link_monitor_to_binder_death_event(
            Arc::clone(&is_healthy),
            "irrelevant".to_string().into(),
            Duration::from_secs(u64::MAX),
            rx,
        )
        .await;

        assert!(result.is_ok());
        assert!(is_healthy.load(Ordering::SeqCst));
        if let Some(callback) = CALLBACK_CAPTURE.lock().unwrap().as_ref() {
            callback();
        } else {
            panic!("Death callback was not captured by test");
        }
        assert!(is_healthy.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn get_binder_timeout_leads_to_unhealthy() {
        let (_, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(SystemTime::now()),
        });
        const BINDER_DELAY: Duration = Duration::from_secs(10);
        let binder_timeout: Duration = Duration::from_secs(2);
        assert!(BINDER_DELAY > binder_timeout);

        pub struct MockBinderInteraction;
        impl BinderInteraction for MockBinderInteraction {
            type DeathCallback = ();
            type BinderObj = ();

            fn wait_for_service(_name: &str) -> Option<Self::BinderObj> {
                thread::sleep(BINDER_DELAY);
                Some(())
            }

            fn create_death_callback(
                _callback: impl Fn() + Send + Sync + 'static,
            ) -> Self::DeathCallback {
            }

            fn link_to_death(
                _binder: &mut Self::BinderObj,
                _callback: &mut Self::DeathCallback,
            ) -> Result<(), binder::StatusCode> {
                Ok(())
            }
        }
        let monitor = AgentHealthMonitor::<MockBinderInteraction>::new(
            "ignored".to_string(),
            binder_timeout,
            rx,
        );
        monitor.poll_with_timeout_for_init_end().await;
        assert!(monitor.get_health_status() == EntityHealthStatus::Unhealthy);
    }
}
