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

use binder::BinderFeatures;
use binder::ExceptionCode;
use binder::Interface;
use binder::Status;
use binder::Strong;
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::BpPowerStateClientApi;
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateClientApi::IPowerStateClientApi;
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateReportListener::BnPowerStateReportListener;
use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateReportListener::IPowerStateReportListener;
use google_sdv_vpm::aidl::google::sdv::vpm::PowerStateReport::PowerStateReport;
use log::{error, info};
use mockall::automock;
use persistent_binder::BinderConnectionManager;
use persistent_binder::PersistentBinderConnection;
use sdv_status_rs::derive;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PowerState {
    Uninitialized,
    Suspended,
    Running,
    ShuttingDown,
}

#[derive(Clone, Copy)]
pub struct PowerStateInfo {
    pub stage: PowerState,
    pub last_resume_time: Option<SystemTime>,
}
impl Default for PowerStateInfo {
    fn default() -> Self {
        Self { stage: PowerState::Uninitialized, last_resume_time: None }
    }
}

pub struct PowerStateReceiverFactoryImpl {
    rx: watch::Receiver<PowerStateInfo>,
    vpm_connection_setup_task: tokio::task::JoinHandle<()>,
}

/// blocking function
fn get_vpm_binder_service() -> Strong<dyn IPowerStateClientApi> {
    let descriptor =
        <BpPowerStateClientApi as IPowerStateClientApi>::get_descriptor().to_owned() + "/default";

    loop {
        match binder::wait_for_interface(&descriptor) {
            Ok(power_state_report_listener) => return power_state_report_listener,
            Err(e) => error!("Failed to get IPowerStateClientApi interface, retrying: {e:?}"),
        }
    }
}

fn subscribe_to_power_state_report(
    power_state_client_service: &Strong<dyn IPowerStateClientApi>,
    power_state_report_listener: &Strong<dyn IPowerStateReportListener>,
) {
    let result = power_state_client_service
        .subscribeToPowerStateReport(power_state_report_listener)
        .and_then(|power_state_report| {
            power_state_report_listener.onPowerStateReportChanged(power_state_report)
        });

    if let Err(e) = result {
        error!("Error subscribing to PowerStateReports: {e}");
    }
}

impl PowerStateReceiverFactoryImpl {
    fn connect_persistently_to_vpm(
        vpm_connection: Arc<impl BinderConnectionManager<dyn IPowerStateClientApi>>,
        tx: watch::Sender<PowerStateInfo>,
        subscribe_to_power_state_report: impl Fn(&Strong<dyn IPowerStateClientApi>, &Strong<dyn IPowerStateReportListener>)
            + Send
            + Sync
            + 'static,
    ) {
        let power_state_report_listener = BnPowerStateReportListener::new_binder(
            PowerStateUpdater::new(tx),
            BinderFeatures::default(),
        );
        {
            let vpm_binder = vpm_connection.get_binder().lock().expect(
                "Lock can't be poisoned, as PersistenBinderConnection accessed from 1 thread",
            );

            subscribe_to_power_state_report(&vpm_binder, &power_state_report_listener);
        };

        vpm_connection.set_reconnect_listener(Box::new(move |vpm_binder| {
            info!("Re-subscribing to PowerStateReports");
            subscribe_to_power_state_report(vpm_binder, &power_state_report_listener);
        }));
    }

    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(PowerStateInfo::default());
        Self {
            rx,
            vpm_connection_setup_task: tokio::task::spawn_blocking(|| {
                Self::connect_persistently_to_vpm(
                    PersistentBinderConnection::new(get_vpm_binder_service),
                    tx,
                    subscribe_to_power_state_report,
                );
            }),
        }
    }
}

impl Drop for PowerStateReceiverFactoryImpl {
    fn drop(&mut self) {
        self.vpm_connection_setup_task.abort();
    }
}
#[automock]
pub trait PowerStateReceiverFactory: Send + Sync {
    fn create_receiver(&self) -> watch::Receiver<PowerStateInfo>;
}

impl PowerStateReceiverFactory for PowerStateReceiverFactoryImpl {
    fn create_receiver(&self) -> watch::Receiver<PowerStateInfo> {
        watch::Receiver::clone(&self.rx) // smart pointer clone, cheap
    }
}

struct PowerStateUpdater {
    tx: watch::Sender<PowerStateInfo>,
}

impl PowerStateUpdater {
    fn new(tx: watch::Sender<PowerStateInfo>) -> Self {
        Self { tx }
    }

    fn update(&self, power_event: &PowerStateReport) -> Result<(), &str> {
        let mut res = Ok(());
        self.tx.send_modify(|state| match power_event {
            &PowerStateReport::SUSPEND_TO_RAM_POST_FINISH
            | &PowerStateReport::SUSPEND_TO_DISK_POST_FINISH => {
                state.last_resume_time = Some(SystemTime::now());
                state.stage = PowerState::Suspended;
            }
            &PowerStateReport::POWER_OFF_POST_FINISH => {
                state.stage = PowerState::ShuttingDown;
            }
            &PowerStateReport::REPORT_UNSPECIFIED => {
                res = Err("Unexpected VPM State transition");
            }
            _ => {
                state.stage = PowerState::Running;
            }
        });
        res
    }
}

impl Interface for PowerStateUpdater {}

#[allow(non_snake_case)]
impl IPowerStateReportListener for PowerStateUpdater {
    /// callback runs on binder thread pool
    fn onPowerStateReportChanged(
        &self,
        power_state_report: PowerStateReport,
    ) -> binder::Result<()> {
        self.update(&power_state_report)
            .map_err(|err| Status::new_exception_str(ExceptionCode::ILLEGAL_STATE, Some(err)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_sdv_vpm::aidl::google::sdv::vpm::IPowerStateReportListener::MockIPowerStateReportListener;
    use mockall::mock;
    use mockall::predicate;
    use persistent_binder::MockBinderConnectionManager;
    use std::sync::Mutex;
    use tokio::sync::watch;

    const VPM_MOCK_POWER_STATE: PowerStateReport = PowerStateReport::ON;
    mock! {
        pub PowerStateClientApi {}
        impl IPowerStateClientApi for PowerStateClientApi {
            #[allow(non_snake_case)]
            fn subscribeToPowerStateReport(&self, listener: &Strong<dyn IPowerStateReportListener>) -> binder::Result<PowerStateReport> {
                Ok(VPM_MOCK_POWER_STATE)
            }
        }
        impl Interface for PowerStateClientApi {}
    }

    #[test]
    fn test_subscribe_power_state_report_subscribes_and_also_reports_current_power_state_to_listener(
    ) {
        let mut mock_power_state_client = MockPowerStateClientApi::new();
        mock_power_state_client
            .expect_subscribeToPowerStateReport()
            .times(1)
            .returning(|_| Ok(PowerStateReport::ON));
        let mock_power_state_client =
            Strong::new(Box::new(mock_power_state_client) as Box<dyn IPowerStateClientApi>);

        let mut mock_listener = MockIPowerStateReportListener::new();
        mock_listener
            .expect_onPowerStateReportChanged()
            .with(predicate::eq(VPM_MOCK_POWER_STATE))
            .times(1)
            .returning(|_| Ok(()));
        let mock_listener =
            Strong::new(Box::new(mock_listener) as Box<dyn IPowerStateReportListener>);

        subscribe_to_power_state_report(&mock_power_state_client, &mock_listener);
    }

    #[test]
    fn coverage_test_connect_persistently_to_vpm() {
        let (tx, _rx) = watch::channel(PowerStateInfo::default());
        let mut mock_vpm_connection = MockBinderConnectionManager::new();
        let mock_power_state_client = Arc::new(Mutex::new(Strong::new(Box::new(
            MockPowerStateClientApi::new(),
        )
            as Box<dyn IPowerStateClientApi>)));
        let nr_calls_to_subscribe = Arc::new(Mutex::new(0));
        let subscribing_logic_mock = {
            let nr_calls_to_subscribe = nr_calls_to_subscribe.clone();
            move |_: &Strong<dyn IPowerStateClientApi>,
                  _: &Strong<dyn IPowerStateReportListener>| {
                *nr_calls_to_subscribe.lock().unwrap() += 1;
            }
        };

        mock_vpm_connection.expect_get_binder().times(1).return_const(mock_power_state_client);
        mock_vpm_connection.expect_set_reconnect_listener().times(1).return_const(());

        PowerStateReceiverFactoryImpl::connect_persistently_to_vpm(
            Arc::new(mock_vpm_connection),
            tx,
            subscribing_logic_mock,
        );

        assert!(*nr_calls_to_subscribe.lock().unwrap() == 1);
    }

    #[test]
    fn test_power_state_updater_initial_state() {
        let (tx, rx) = watch::channel(PowerStateInfo::default());
        let _updater = PowerStateUpdater::new(tx);

        assert!(matches!(rx.borrow().stage, PowerState::Uninitialized));
    }

    #[test]
    fn test_power_state_updater_uninitialized_to_running() {
        let (tx, rx) = watch::channel(PowerStateInfo::default());
        let updater = PowerStateUpdater::new(tx);

        updater.update(&PowerStateReport::POWER_OFF_EXIT).unwrap();
        assert!(matches!(rx.borrow().stage, PowerState::Running));
    }

    #[test]
    fn test_power_state_updater_running_to_suspended() {
        let (tx, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(SystemTime::now()),
        });
        let updater = PowerStateUpdater::new(tx);

        updater.update(&PowerStateReport::SUSPEND_TO_RAM_POST_FINISH).unwrap();
        assert!(matches!(rx.borrow().stage, PowerState::Suspended));
    }

    #[test]
    fn test_power_state_updater_suspended_to_running() {
        let (tx, rx) =
            watch::channel(PowerStateInfo { stage: PowerState::Suspended, last_resume_time: None });
        let updater = PowerStateUpdater::new(tx);

        updater.update(&PowerStateReport::SUSPEND_TO_RAM_EXIT).unwrap();
        assert!(matches!(rx.borrow().stage, PowerState::Running));
    }

    #[test]
    fn test_power_state_updater_and_an_invalid_transition() {
        let (tx, rx) =
            watch::channel(PowerStateInfo { stage: PowerState::Suspended, last_resume_time: None });
        let updater = PowerStateUpdater::new(tx);

        let result = updater.update(&PowerStateReport::REPORT_UNSPECIFIED);
        assert!(result.is_err());
        assert!(matches!(rx.borrow().stage, PowerState::Suspended));
    }

    #[test]
    fn test_power_state_updater_running_no_change() {
        let (tx, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(SystemTime::now()),
        });
        let updater = PowerStateUpdater::new(tx);

        updater.update(&PowerStateReport::ON).unwrap();
        assert!(matches!(rx.borrow().stage, PowerState::Running));
    }

    #[test]
    fn test_power_state_updater_to_shutting_down() {
        let (tx, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(SystemTime::now()),
        });
        let updater = PowerStateUpdater::new(tx);

        updater.update(&PowerStateReport::POWER_OFF_POST_FINISH).unwrap();
        assert!(matches!(rx.borrow().stage, PowerState::ShuttingDown));
    }

    #[test]
    fn test_power_state_updater_shutting_down_to_running() {
        let (tx, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::ShuttingDown,
            last_resume_time: None,
        });
        let updater = PowerStateUpdater::new(tx);

        updater.update(&PowerStateReport::ON).unwrap();
        assert!(matches!(rx.borrow().stage, PowerState::Running));
    }
}
