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
use crate::monitor_model::EntityHealthStatus;
use crate::monitor_model::EntityMonitor;
use crate::power_state::PowerState;
use crate::power_state::PowerStateInfo;
use futures::future;
use futures::stream;
use futures::StreamExt;
use health_api::health_topic::ServiceHeartbeat;
use log::{error, warn};
use protobuf::well_known_types::timestamp::Timestamp;
use sdv_dump::DumpStateReporter;
use sdv_status_rs::SdvResult;
use std::cmp;
use std::pin::Pin;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;

/// For backwards compatibility, logic uses SystemTime API.
/// In the future, consider switching to structures built around monotonic clocks,
/// I.e. std::time::Instant
#[derive(Clone, Copy, Debug, PartialEq)]
enum HeartbeatEvent {
    StartedMonitoring(SystemTime),
    ReceivedHearbeat(SystemTime),
}

impl HeartbeatEvent {
    fn get_event_time(self) -> SystemTime {
        match self {
            HeartbeatEvent::StartedMonitoring(t) => t,
            HeartbeatEvent::ReceivedHearbeat(t) => t,
        }
    }
}

pub struct HeartbeatMonitor {
    heartbeat_listener_task: tokio::task::JoinHandle<()>,
    last_event: watch::Receiver<HeartbeatEvent>,
    config: MonitorConfig,
    current_time_provider: Box<dyn Send + Sync + 'static + Fn() -> SystemTime>,
    power_state_receiver: watch::Receiver<PowerStateInfo>,
}

impl Drop for HeartbeatMonitor {
    fn drop(&mut self) {
        self.heartbeat_listener_task.abort();
    }
}

impl HeartbeatMonitor {
    pub fn new(
        heartbeat_publication: impl futures::stream::Stream<Item = SdvResult<Vec<ServiceHeartbeat>>>
            + Send
            + Unpin
            + 'static,
        health_config: HeartbeatMonitoringConfig,
        current_time_provider: Box<dyn Fn() -> SystemTime + Send + Sync + 'static>,
        power_state_receiver: watch::Receiver<PowerStateInfo>,
    ) -> HeartbeatMonitor {
        let (tx, rx) =
            tokio::sync::watch::channel(HeartbeatEvent::StartedMonitoring(current_time_provider()));
        Self {
            heartbeat_listener_task: tokio::task::spawn(async move {
                heartbeat_publication
                    .into_sanitized_heartbeats()
                    .for_each(|hb| {
                        tx.send_replace(HeartbeatEvent::ReceivedHearbeat(hb));
                        future::ready(())
                    })
                    .await;
            }),
            config: health_config.into(),
            last_event: rx,
            current_time_provider,
            power_state_receiver,
        }
    }
}

impl DumpStateReporter for HeartbeatMonitor {
    fn dump(
        &self,
        writer: &mut dyn std::io::Write,
        _args: &[&std::ffi::CStr],
    ) -> Result<(), std::io::Error> {
        writeln!(
            writer,
            "is_healthy: {:?}\n\
            last_hb_event: {:?}\n\
            monitor_alive: {}\n",
            self.get_health_status(),
            *self.last_event.borrow(),
            !self.heartbeat_listener_task.is_finished()
        )
    }
}

impl EntityMonitor for HeartbeatMonitor {
    fn get_config(&self) -> &HeartbeatMonitoringConfig {
        &self.config.service_config
    }

    fn get_health_status(&self) -> EntityHealthStatus {
        let last_suspend_time = {
            let power_state = *self.power_state_receiver.borrow();
            if power_state.stage == PowerState::Suspended {
                return EntityHealthStatus::Healthy;
            }
            power_state.last_resume_time
        };

        let last_event = *self.last_event.borrow();
        let hb_event_time = last_event.get_event_time();
        let current_time = (self.current_time_provider)();
        let allowed_delay = match last_event {
            HeartbeatEvent::StartedMonitoring(_) => self.config.allowed_delay_since_monitor_start,
            HeartbeatEvent::ReceivedHearbeat(_) => self.config.allowed_delay_since_last_heartbeat,
        };

        let elapsed = current_time
            .duration_since(cmp::max(
                last_suspend_time.unwrap_or(SystemTime::UNIX_EPOCH),
                hb_event_time,
            ))
            .unwrap_or_else(|err| {
                warn!(
                    "Non monotonic clock issues: {err:?}, when computing duration between \
                last event and current time"
                );
                Duration::ZERO
            });
        if elapsed < allowed_delay {
            EntityHealthStatus::Healthy
        } else {
            EntityHealthStatus::Unhealthy
        }
    }
}

trait HeartbeatStream {
    fn into_sanitized_heartbeats(
        self,
    ) -> Pin<Box<dyn futures::stream::Stream<Item = SystemTime> + Send>>;
}

impl<T> HeartbeatStream for T
where
    T: futures::stream::Stream<Item = SdvResult<Vec<ServiceHeartbeat>>> + Send + Unpin + 'static,
{
    fn into_sanitized_heartbeats(
        self,
    ) -> Pin<Box<dyn futures::stream::Stream<Item = SystemTime> + Send>> {
        let s = self
            .filter_map(|r| async move {
                if let Ok(hbs) = r {
                    Some(hbs)
                } else {
                    warn!("Heartbeat stream unexpectedly returned {r:?}. Continuing monitoring.");
                    None
                }
            })
            .flat_map(stream::iter)
            .map(|hb| hb.timestamp.unwrap_or_default().into())
            .scan(Timestamp::default().into(), |last_hb, new_hb| {
                let r = if new_hb >= *last_hb {
                    *last_hb = new_hb;
                    Some(Some(new_hb))
                } else {
                    warn!("Non monotonic clock issue: received out of order heartbeat. Ignoring.");
                    Some(None)
                };
                future::ready(r)
            })
            .filter_map(future::ready);
        Box::pin(s)
    }
}

struct MonitorConfig {
    service_config: HeartbeatMonitoringConfig,
    allowed_delay_since_last_heartbeat: Duration,
    allowed_delay_since_monitor_start: Duration,
}

impl From<HeartbeatMonitoringConfig> for MonitorConfig {
    fn from(val: HeartbeatMonitoringConfig) -> MonitorConfig {
        MonitorConfig {
            allowed_delay_since_last_heartbeat: Duration::from_millis(
                val.period_ms
                    .checked_mul(val.num_periods)
                    .and_then(|u| u.checked_add(val.task_duration_ms))
                    .unwrap_or_else(|| {
                        let max = u64::MAX;
                        error!("Heartbeat configuration u64 overflow. Clamping value to {max} ms",);
                        max
                    }),
            ),

            allowed_delay_since_monitor_start: Duration::from_millis(val.initial_delay_ms),
            service_config: val,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use protobuf::well_known_types::timestamp::Timestamp;
    use protobuf::MessageField;
    use sdv_status_rs::{SdvStatus, SdvStatusCode};
    use std::time::SystemTime;

    mod utils {
        use super::*;
        use sdv_status_rs::SdvStatus;
        use std::sync::Mutex;
        use tokio_stream::wrappers::WatchStream;
        pub struct MockCurrentTimeProvider {
            times: Vec<SystemTime>,
            current_time_index: Mutex<usize>,
        }
        impl MockCurrentTimeProvider {
            pub fn new(times: Vec<SystemTime>) -> Self {
                Self { times, current_time_index: 0.into() }
            }
            pub fn now(&self) -> SystemTime {
                let mut index = self.current_time_index.lock().unwrap();
                let r = self
                    .times
                    .get(*index)
                    .expect("MockCurrentTime called more than expected, check test logic");
                *index += 1;
                *r
            }
        }

        pub fn create_mock_controllable_hb_stream() -> (
            watch::Sender<Result<Vec<ServiceHeartbeat>, SdvStatusCode>>,
            Pin<Box<dyn futures::stream::Stream<Item = SdvResult<Vec<ServiceHeartbeat>>> + Send>>,
        ) {
            let (tx, rx) = watch::channel(Ok(vec![]));
            (tx, Box::pin(WatchStream::from_changes(rx).map(|hbs| hbs.map_err(SdvStatus::new))))
        }

        impl HeartbeatMonitor {
            /// Useful for waiting until async hb stream input was propagated, ensuring
            /// reproductibility
            pub async fn wait_for_hb_with_timeout(
                &mut self,
                expected_hb_event: &ServiceHeartbeat,
            ) -> std::result::Result<(), ()> {
                let expected_hb_event = HeartbeatEvent::ReceivedHearbeat(
                    expected_hb_event.timestamp.clone().unwrap_or_default().into(),
                );

                tokio::time::timeout(
                    Duration::from_secs(5),
                    self.last_event.wait_for(|event| event == &expected_hb_event),
                )
                .await
                .map(|_| ())
                .map_err(|_| ())
            }
        }
    }

    #[tokio::test]
    async fn monitor_is_healthy_upon_creation() {
        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let monitor = HeartbeatMonitor::new(
            stream::pending(),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 100,
                period_ms: 10,
                num_periods: 10,
                task_duration_ms: 2,
            },
            Box::new(|| Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into()),
            rx,
        );
        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy)
    }

    #[tokio::test]
    async fn configs_equal() {
        let config = HeartbeatMonitoringConfig {
            initial_delay_ms: 100,
            period_ms: 10,
            num_periods: 10,
            task_duration_ms: 2,
        };
        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let monitor =
            HeartbeatMonitor::new(stream::pending(), config.clone(), Box::new(SystemTime::now), rx);
        assert_eq!(*monitor.get_config(), config);
    }

    #[tokio::test]
    async fn configs_differ() {
        let config = HeartbeatMonitoringConfig {
            initial_delay_ms: 100,
            period_ms: 10,
            num_periods: 10,
            task_duration_ms: 2,
        };
        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let monitor = HeartbeatMonitor::new(
            stream::pending(),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 1000,
                period_ms: 10,
                num_periods: 10,
                task_duration_ms: 2,
            },
            Box::new(SystemTime::now),
            rx,
        );
        assert!(*monitor.get_config() != config);
    }

    #[tokio::test]
    async fn monitor_is_healthy_during_initial_delay() {
        // registration    |---------------
        // init delay      -------|--------
        // get_health time ----|-----------
        // heartbeats      ----------|--|--
        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(), // retrieved on monitor constructor
            Timestamp { seconds: 1, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status
        ]);

        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let monitor = HeartbeatMonitor::new(
            stream::pending(),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 2000,
                period_ms: 100,
                num_periods: 1000,
                task_duration_ms: 10,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy)
    }

    #[tokio::test]
    async fn monitor_is_unhealthy_after_initial_delay_with_no_heartbeats() {
        // registration |---------------
        // init delay    -------|-------
        // current_time  -----------|---

        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(), // retrieved on monitor constructor
            Timestamp { seconds: 1, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status, expect unhealthy
        ]);

        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let monitor = HeartbeatMonitor::new(
            stream::pending(),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 500,
                period_ms: 100,
                num_periods: 1,
                task_duration_ms: 10,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        assert!(monitor.get_health_status() == EntityHealthStatus::Unhealthy)
    }

    #[tokio::test]
    async fn monitor_is_unhealthy_when_heartbeat_arrives_after_monitored_period() {
        // registration |-------------------
        // init delay    -------|-----------
        // heartbeat     -----------|-------
        // health check  -----------------|-
        // allowed delay            |--|
        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(), // retrieved on monitor constructor
            Timestamp { seconds: 3, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status, expect unhealthy
        ]);
        let hb = ServiceHeartbeat {
            timestamp: MessageField::some(Timestamp { seconds: 1, ..Default::default() }),
            ..Default::default()
        };

        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let mut monitor = HeartbeatMonitor::new(
            stream::once(future::ready(Ok(vec![hb.clone()]))),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 500,
                period_ms: 100,
                num_periods: 1,
                task_duration_ms: 10,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        monitor.wait_for_hb_with_timeout(&hb).await.expect("HB should be received during timeout");
        assert!(monitor.get_health_status() == EntityHealthStatus::Unhealthy)
    }

    #[tokio::test]
    async fn monitor_becomes_unhealthy_after_missing_second_heartbeat() {
        // registration |---------------
        // heartbeat     -----|---------
        // health check  -----|------|--
        let hb = ServiceHeartbeat {
            timestamp: MessageField::some(Timestamp { seconds: 1, ..Default::default() }),
            ..Default::default()
        };

        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(), // retrieved on monitor constructor
            Timestamp { seconds: 1, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status, expect healthy
            Timestamp { seconds: 3, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status, expect unhealthy
        ]);

        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let mut monitor = HeartbeatMonitor::new(
            stream::once(future::ready(Ok(vec![hb.clone()]))),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 500,
                period_ms: 100,
                num_periods: 1,
                task_duration_ms: 10,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        monitor.wait_for_hb_with_timeout(&hb).await.expect("HB should be received during timeout");

        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy);
        assert!(monitor.get_health_status() == EntityHealthStatus::Unhealthy)
    }

    #[tokio::test]
    async fn monitor_is_healthy_when_heartbeat_arrives_at_end_of_monitored_period() {
        // registration |---------------
        // heartbeat     -----|---------
        // health check  -----|---------
        let hb = ServiceHeartbeat {
            timestamp: MessageField::some(Timestamp { seconds: 1, ..Default::default() }),
            ..Default::default()
        };
        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(), // retrieved on monitor constructor
            Timestamp { seconds: 1, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status
        ]);

        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let mut monitor = HeartbeatMonitor::new(
            stream::once(future::ready(Ok(vec![hb.clone()]))),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 500,
                period_ms: 100,
                num_periods: 1,
                task_duration_ms: 10,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        monitor.wait_for_hb_with_timeout(&hb).await.expect("Expecting hb to arrive before timeout");

        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy)
    }

    #[tokio::test]
    async fn monitor_becomes_unhealthy_only_after_missing_num_periods_heartbeats() {
        // registration |---------------
        // heartbeat     -----|---------
        // health check  -----|-|-|-|--|--
        // time               1s 2s 3s 4s
        // status        healthy--------|unhealthy
        let hb = ServiceHeartbeat {
            timestamp: MessageField::some(Timestamp { seconds: 1, ..Default::default() }),
            ..Default::default()
        };

        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(), // retrieved on monitor constructor
            Timestamp { seconds: 1, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status
            Timestamp { seconds: 2, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status
            Timestamp { seconds: 3, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status
            Timestamp { seconds: 4, nanos: 0, ..Default::default() }.into(), // retrieved on get_health_status
            Timestamp {
                seconds: 4,
                nanos: 1,
                // retrieved on get_health_status, see hb timestamp and hm config, 1ns after allowed delay
                ..Default::default()
            }
            .into(),
        ]);

        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let mut monitor = HeartbeatMonitor::new(
            stream::once(future::ready(Ok(vec![hb.clone()]))),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 1000,
                period_ms: 1000,
                num_periods: 3,
                task_duration_ms: 0,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        monitor.wait_for_hb_with_timeout(&hb).await.expect("Did not receive hb in timeout");

        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy);
        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy);
        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy);
        assert!(monitor.get_health_status() == EntityHealthStatus::Unhealthy);
        // see mock_time
    }

    #[tokio::test]
    async fn monitor_is_healthy_between_two_heartbeats_with_delay_larger_than_period_and_smaller_than_total_delay(
    ) {
        // heartbeats    -|-|--|--
        // health check  --|----|-

        // In this scenario based on a bug report, the delay between 2 heartbeats is 6 ms, while the period is only 5 ms.
        // We still have to consider the task duration and number of periods which make the allowed delay to be 10 ms.
        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(), // retrieved on monitor constructor
            Timestamp { seconds: 1737110450, nanos: 290393482, ..Default::default() }.into(), // retrieved on get_health_status
        ]);

        let hb_s = 1737110450;
        let hb_ns = 284227137;
        let hb = ServiceHeartbeat {
            timestamp: MessageField::some(Timestamp {
                seconds: hb_s,
                nanos: hb_ns,
                ..Default::default()
            }),
            ..Default::default()
        };
        let hb_stream = stream::once(future::ready(Ok(vec![hb.clone()])));

        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let mut monitor = HeartbeatMonitor::new(
            hb_stream,
            HeartbeatMonitoringConfig {
                initial_delay_ms: 50,
                period_ms: 5,
                num_periods: 1,
                task_duration_ms: 5,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        monitor
            .wait_for_hb_with_timeout(&hb)
            .await
            .expect("Did not received expected hb in timeout");

        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn healthy_when_beat_interval_longer_than_config_but_last_beat_vm_report_diff_smaller_than_config(
    ) {
        // valid heartbeat interval
        // (period_ms*num_periods + task_duration_ms)     |--|
        // heartbeats                                     ------|--------|---------|--
        // health check                                   -------|--------|---------|

        let first_hb = ServiceHeartbeat {
            timestamp: MessageField::some(Timestamp { seconds: 0, nanos: 0, ..Default::default() }),
            ..Default::default()
        };
        let second_hb = ServiceHeartbeat {
            timestamp: MessageField::some(Timestamp { seconds: 1, nanos: 0, ..Default::default() }),
            ..Default::default()
        };

        let (sender, stream) = utils::create_mock_controllable_hb_stream();
        let time_mock = utils::MockCurrentTimeProvider::new(vec![
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(),
            Timestamp { seconds: 0, nanos: 1, ..Default::default() }.into(),
            Timestamp { seconds: 1, nanos: 1, ..Default::default() }.into(),
        ]);
        let time_provider = Box::new(move || time_mock.now());

        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let mut hb = HeartbeatMonitor::new(
            stream,
            HeartbeatMonitoringConfig {
                initial_delay_ms: 1,
                period_ms: 100,
                num_periods: 1,
                task_duration_ms: 0,
            },
            time_provider,
            rx,
        );
        sender.send(Ok(vec![first_hb.clone()])).unwrap();
        hb.wait_for_hb_with_timeout(&first_hb)
            .await
            .expect("timeout, first_hb not received by monitor internals");

        // expecting healthy post this heartbeat: it is the initial heartbeat happening post
        // registration, happens in initial_delay_ms interval
        assert!(hb.get_health_status() == EntityHealthStatus::Healthy);
        sender.send(Ok(vec![second_hb.clone()])).unwrap();
        hb.wait_for_hb_with_timeout(&second_hb)
            .await
            .expect("timeout, first_hb not received by monitor internals");

        // STILL expecting healthy: although the SB reports heartbeats at a lower rate than it's
        // configuration, its last heartbeat is still in "valid heartbeat interval" from point of
        // view of HM. Can be interpreted as: SB managed to recover from its unhealthyness right
        // before the VM health is reported
        assert!(hb.get_health_status() == EntityHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn monitor_is_healthy_when_hb_stream_signals_data_loss() {
        let final_hb = ServiceHeartbeat {
            timestamp: MessageField::some(Timestamp { seconds: 0, nanos: 5, ..Default::default() }),
            ..Default::default()
        };
        let hb_stream = stream::iter([
            Ok(vec![
                ServiceHeartbeat {
                    timestamp: MessageField::some(Timestamp {
                        seconds: 0,
                        nanos: 1,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ServiceHeartbeat {
                    timestamp: MessageField::some(Timestamp {
                        seconds: 0,
                        nanos: 2,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ]),
            Err(SdvStatus::new(SdvStatusCode::DataLoss)),
            Ok(vec![
                ServiceHeartbeat {
                    timestamp: MessageField::some(Timestamp {
                        seconds: 0,
                        nanos: 4,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                final_hb.clone(),
            ]),
        ]);
        let time_mock = utils::MockCurrentTimeProvider::new(vec![
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(),
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(),
        ]);

        let (_tx, rx) = watch::channel(PowerStateInfo::default());
        let mut monitor = HeartbeatMonitor::new(
            hb_stream,
            HeartbeatMonitoringConfig {
                initial_delay_ms: 500,
                period_ms: 100,
                num_periods: 1,
                task_duration_ms: 10,
            },
            Box::new(move || time_mock.now()),
            rx,
        );
        monitor
            .wait_for_hb_with_timeout(&final_hb)
            .await
            .expect("Did not receive hb during timeout");

        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy)
    }

    #[tokio::test]
    async fn monitor_is_healthy_during_suspend() {
        let period_ms = 1000;
        let num_periods = 1;
        let hm_construct_time: SystemTime =
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into();
        let health_report_time: SystemTime =
            Timestamp { seconds: 20, nanos: 0, ..Default::default() }.into();

        // missed heartbeat condition:
        assert!(
            health_report_time.duration_since(hm_construct_time).unwrap().as_millis() as u64
                > period_ms * num_periods
        );

        let mock_time =
            utils::MockCurrentTimeProvider::new(vec![hm_construct_time, health_report_time]);

        let (_tx, rx) =
            watch::channel(PowerStateInfo { stage: PowerState::Suspended, last_resume_time: None });
        let monitor = HeartbeatMonitor::new(
            stream::pending(),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 0,
                period_ms,
                num_periods,
                task_duration_ms: 0,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn monitor_is_healthy_post_suspend_registration_only_presuspend() {
        // Timeline:
        // T=0                    T=19.5s    T=20s            T=20.5s
        // |------...suspend...------|----------|---------------|
        // Registration            Resume     Health Check    Initial Delay End
        //
        // Effective time since registration = 20s - 19.5s = 0.5s.
        // Initial delay is 1s. Since 0.5s < 1s, the status is Healthy.

        let registration_time: SystemTime =
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into();
        let resume_time: SystemTime =
            Timestamp { seconds: 19, nanos: 500_000_000, ..Default::default() }.into();
        let time_of_health_check: SystemTime =
            Timestamp { seconds: 20, nanos: 0, ..Default::default() }.into();
        let initial_delay_ms = 1000;

        assert!(
            (time_of_health_check.duration_since(resume_time).unwrap().as_millis() as u64)
                < initial_delay_ms
        );

        let mock_time =
            utils::MockCurrentTimeProvider::new(vec![registration_time, time_of_health_check]);

        let (_tx, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(resume_time),
        });
        let monitor = HeartbeatMonitor::new(
            stream::pending(),
            HeartbeatMonitoringConfig {
                initial_delay_ms,
                period_ms: 0,
                num_periods: 0,
                task_duration_ms: 0,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy);
    }

    #[tokio::test]
    async fn monitor_is_healthy_shortly_post_suspend_hb_received_presuspend() {
        // Timeline:
        // T=1s                    T=21s    T=21.5s            T=22.5s
        // |------...suspend...------|----------|---------------|
        // Last Heartbeat         Resume     Health Check    Heartbeat Delay End
        //
        // Time since resume event(effectively treated as a HB) = 21.5s - 21s = 0.5s.
        // Allowed delay between HBs is 1s. Since 0.5s < 1s, the status is Healthy.

        let allowed_hb_interval_ms = 1000;
        let hb_time: SystemTime = Timestamp { seconds: 1, ..Default::default() }.into();
        let resume_time: SystemTime = Timestamp { seconds: 21, ..Default::default() }.into();
        let health_check_time: SystemTime =
            Timestamp { seconds: 21, nanos: 500_000_000, ..Default::default() }.into();

        assert!(
            (health_check_time.duration_since(resume_time).unwrap().as_millis() as u64)
                < allowed_hb_interval_ms
        );

        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            // retrieved on monitor construction:
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(),
            // retrieved on health check:
            health_check_time,
        ]);
        let hb = ServiceHeartbeat {
            timestamp: MessageField::some(hb_time.into()),
            ..Default::default()
        };

        let (_tx, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(resume_time),
        });
        let mut monitor = HeartbeatMonitor::new(
            stream::once(future::ready(Ok(vec![hb.clone()]))),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 0,
                period_ms: allowed_hb_interval_ms,
                num_periods: 1,
                task_duration_ms: 0,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        monitor.wait_for_hb_with_timeout(&hb).await.expect("HB should be received during timeout");

        assert!(monitor.get_health_status() == EntityHealthStatus::Healthy)
    }
    #[tokio::test]
    async fn monitor_is_unhealthy_post_suspend_new_hb_not_received() {
        // Timeline:
        // T=1s                    T=21s      T=22s            T=23s
        // |------...suspend...------|----------|------------------|
        // Last Heartbeat         Resume   Heartbeat Delay End    Health check
        //
        // Time since resume event(effectively treated as a HB) = 23s - 22s = 0.5s.
        // Allowed delay between HBs is 1s. Since 2s > 1s, the status is Unhealthy.

        let allowed_hb_interval_ms = 1000;
        let hb_time: SystemTime = Timestamp { seconds: 1, ..Default::default() }.into();
        let resume_time: SystemTime = Timestamp { seconds: 21, ..Default::default() }.into();
        let health_check_time: SystemTime = Timestamp { seconds: 23, ..Default::default() }.into();

        assert!(
            (health_check_time.duration_since(resume_time).unwrap().as_millis() as u64)
                > allowed_hb_interval_ms
        );

        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            // retrieved on monitor constructor:
            Timestamp { seconds: 0, nanos: 0, ..Default::default() }.into(),
            // retrieved on get_health_status:
            health_check_time,
        ]);
        let hb = ServiceHeartbeat {
            timestamp: MessageField::some(hb_time.into()),
            ..Default::default()
        };

        let (_tx, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::Running,
            last_resume_time: Some(resume_time),
        });
        let mut monitor = HeartbeatMonitor::new(
            stream::once(future::ready(Ok(vec![hb.clone()]))),
            HeartbeatMonitoringConfig {
                initial_delay_ms: 0,
                period_ms: allowed_hb_interval_ms,
                num_periods: 1,
                task_duration_ms: 0,
            },
            Box::new(move || mock_time.now()),
            rx,
        );
        monitor.wait_for_hb_with_timeout(&hb).await.expect("HB should be received during timeout");

        assert!(monitor.get_health_status() == EntityHealthStatus::Unhealthy);
    }

    #[tokio::test]
    async fn monitor_is_unhealthy_if_hbs_missed_regardless_of_shutdown_stage() {
        // Setup a configuration that would otherwise result in Unhealthy status
        let config = HeartbeatMonitoringConfig {
            initial_delay_ms: 100,
            period_ms: 100,
            num_periods: 1,
            task_duration_ms: 0,
        };

        // Mock time to simulate that the initial delay has passed
        let mock_time = utils::MockCurrentTimeProvider::new(vec![
            SystemTime::UNIX_EPOCH,                              // construction time
            SystemTime::UNIX_EPOCH + Duration::from_millis(200), // health check time
        ]);

        // Set power state to ShuttingDown
        let (_tx, rx) = watch::channel(PowerStateInfo {
            stage: PowerState::ShuttingDown,
            last_resume_time: Some(SystemTime::UNIX_EPOCH),
        });

        let monitor = HeartbeatMonitor::new(
            stream::pending(), // No heartbeats
            config,
            Box::new(move || mock_time.now()),
            rx,
        );

        // Monitor report unhealthy as no hbs were received, REGARDLESS of shutdown state
        assert!(monitor.get_health_status() == EntityHealthStatus::Unhealthy);
    }
}
