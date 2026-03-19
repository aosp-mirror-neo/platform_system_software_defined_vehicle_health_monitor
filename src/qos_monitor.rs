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
#![allow(unused_variables)]

use crate::qos_config::QosConfig;
use futures::Stream;
use futures::StreamExt;
use health_api::qos_heartbeat::QosHeartbeat;
use log::error;
use sdv_dump::DumpStateReporter;
use sdv_status_rs::SdvResult;
use sdv_status_rs::{SdvStatus, SdvStatusCode};
use std::time::SystemTime;
use tap::Pipe;
use tokio::sync::watch;
use tokio::time::Duration;

/// For backwards compatibility, logic uses SystemTime API.
/// In the future, consider switching to structures built around monotonic clocks,
/// I.e. std::time::Instant
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QosTick {
    latency: Duration,
    received_time: SystemTime,
}
impl Default for QosTick {
    fn default() -> Self {
        Self { latency: Duration::ZERO, received_time: SystemTime::now() }
    }
}

#[derive(Debug, PartialEq)]
pub enum QosStatus {
    Healthy,
    UnhealthyLatencyViolation,
    UnhealthyWireFrequencyViolation,
    UnhealthyHBFrequencyViolation,
}

pub struct QosMonitor {
    listening_task: tokio::task::JoinHandle<()>,
    latest_ticks: watch::Receiver<(Option<SdvResult<QosTick>>, SdvResult<QosTick>)>,
    config: QosConfig,
    current_time_provider: Box<dyn Send + Sync + 'static + Fn() -> SystemTime>,
}

impl QosMonitor {
    pub fn new(
        sud_discovered_tick: SdvResult<QosTick>,
        qos_stream: impl Stream<Item = SdvResult<QosTick>> + Send + 'static,
        qos_config: QosConfig,
        current_time_provider: Box<dyn Send + Sync + 'static + Fn() -> SystemTime>,
    ) -> Self {
        let (tx, rx) = tokio::sync::watch::channel((None, sud_discovered_tick));
        let listening_task = tokio::spawn(async move {
            qos_stream
                .for_each(async |curr| {
                    let prev = {
                        let guard = tx.borrow();
                        match &guard.1 {
                            Ok(prev) => Ok(*prev),
                            Err(e) => Err(SdvStatus::new(e.status_code())),
                        }
                    };
                    let _ = tx.send_replace((Some(prev), curr));
                })
                .await;
        });
        Self { listening_task, latest_ticks: rx, config: qos_config, current_time_provider }
    }

    pub fn get_status(&self) -> SdvResult<QosStatus> {
        let (previous_tick, current_tick) = match &*self.latest_ticks.borrow() {
            (Some(Ok(prev)), Ok(last)) => (Some(*prev), *last),
            (None, Ok(last)) => (None, *last),
            (prev, last) => {
                error!("Error found on qos hb stream. Contents: {prev:?}, {last:?}");
                return Err(SdvStatus::new(SdvStatusCode::InvalidArgument));
            }
        };

        if check_hb_frequency_violation(
            self.config.frequency_threshold,
            current_tick,
            (self.current_time_provider)(),
        )? {
            return Ok(QosStatus::UnhealthyHBFrequencyViolation);
        }
        if check_wire_latency_violation(self.config.latency_threshold, current_tick) {
            return Ok(QosStatus::UnhealthyLatencyViolation);
        }
        if check_wire_frequency_violation(
            self.config.frequency_threshold,
            previous_tick,
            current_tick,
        )? {
            return Ok(QosStatus::UnhealthyWireFrequencyViolation);
        }
        Ok(QosStatus::Healthy)
    }
}

fn check_hb_frequency_violation(
    frequency_threshold: Option<Duration>,
    latest_tick: QosTick,
    current_time: SystemTime,
) -> SdvResult<bool> {
    let frequency_threshold = if let Some(t) = frequency_threshold {
        t
    } else {
        return Ok(false); // Qos monitor not configured to check frequency
    };

    let time_since_last_hb =
        current_time.duration_since(latest_tick.received_time).map_err(|e| {
            SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                format!("Qos heartbeats out of order, hb frequency check. Error: {e:?}"),
            )
        })?;
    Ok(time_since_last_hb > frequency_threshold)
}

fn check_wire_latency_violation(
    latency_threshold: Option<Duration>,
    current_tick: QosTick,
) -> bool {
    let latency_threshold = if let Some(t) = latency_threshold {
        t
    } else {
        return false; // Qos monitor not configured to check frequency
    };
    current_tick.latency > latency_threshold
}

fn check_wire_frequency_violation(
    frequency_threshold: Option<Duration>,
    previous_tick: Option<QosTick>,
    current_tick: QosTick,
) -> SdvResult<bool> {
    let previous_tick = if let Some(p) = previous_tick {
        p
    } else {
        return Ok(false); // No previous tick, at beginning of monitoring, nothing to check yet
    };
    let frequency_threshold = if let Some(t) = frequency_threshold {
        t
    } else {
        return Ok(false); // Qos monitor not configured to check frequency
    };
    let interval_between_messages_received =
        current_tick.received_time.duration_since(previous_tick.received_time).map_err(|e| {
            SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                format!("Qos heartbeats out of order, wire frequency check. Error: {e:?}"),
            )
        })?;
    Ok(interval_between_messages_received > frequency_threshold)
}

impl Drop for QosMonitor {
    fn drop(&mut self) {
        self.listening_task.abort();
    }
}

impl DumpStateReporter for QosMonitor {
    fn dump(
        &self,
        writer: &mut dyn std::io::Write,
        _args: &[&std::ffi::CStr],
    ) -> Result<(), std::io::Error> {
        writeln!(
            writer,
            "MONITOR INTERNAL DATA:\n\
            health status: {:?}\n\
            task running: {}\n",
            self.get_status(),
            !self.listening_task.is_finished()
        )
    }
}

impl TryFrom<QosHeartbeat> for QosTick {
    type Error = SdvStatus;

    fn try_from(value: QosHeartbeat) -> Result<Self, Self::Error> {
        let data_sent_timestamp: SystemTime = value
            .data_timestamp
            .into_option()
            .ok_or(SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                "Qos heartbeat: received empty data sent timestamp",
            ))?
            .pipe(Into::into);
        let data_received_timestamp: SystemTime = value
            .timestamp
            .into_option()
            .ok_or(SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                "Qos heartbeat: received empty data sent timestamp",
            ))?
            .pipe(Into::into);
        let latency = data_received_timestamp.duration_since(data_sent_timestamp).map_err(|e| {
            SdvStatus::with_message(
                SdvStatusCode::InvalidArgument,
                format!("Qos monitoring: data should be sent before being received {e:?}"),
            )
        })?;

        Ok(Self { latency, received_time: data_received_timestamp })
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;
    use futures::stream;
    use protobuf::well_known_types::timestamp::Timestamp;
    use protobuf::MessageField;
    use std::time::Duration;

    impl QosMonitor {
        /// Useful for waiting until async hb stream input was propagated, ensuring
        /// reproductibility
        async fn wait_for_qos_ticks_with_timeout(
            &mut self,
            expected_ticks: &(Option<SdvResult<QosTick>>, SdvResult<QosTick>),
        ) -> std::result::Result<(), ()> {
            let matches = |r1: &SdvResult<QosTick>, r2: &SdvResult<QosTick>| match (r1, r2) {
                (Ok(t1), Ok(t2)) => t1 == t2,
                (Err(s1), Err(s2)) => s1.status_code() == s2.status_code(),
                _ => false,
            };
            tokio::time::timeout(
                Duration::from_secs(1),
                self.latest_ticks.wait_for(|event| {
                    let prev_matches = match (&event.0, &expected_ticks.0) {
                        (Some(r1), Some(r2)) => matches(r1, r2),
                        (None, None) => true,
                        _ => false,
                    };
                    prev_matches && matches(&event.1, &expected_ticks.1)
                }),
            )
            .await
            .map(|_| ())
            .map_err(|_| ())
        }
    }

    #[tokio::test]
    async fn test_get_status_healthy() {
        let now: SystemTime = Timestamp { seconds: 500, nanos: 0, ..Default::default() }.into();
        let mock_time_provider = Box::new(move || now);

        let config = QosConfig {
            latency_threshold: Some(Duration::from_millis(160)),
            frequency_threshold: Some(Duration::from_millis(200)),
        };
        let tick1 = QosTick {
            latency: Duration::from_millis(50),
            received_time: now - Duration::from_millis(250),
        };
        let tick2 = QosTick {
            latency: Duration::from_millis(50),
            received_time: now - Duration::from_millis(100),
        };
        let expected = (Some(Ok(tick1)), Ok(tick2));

        let mut monitor =
            QosMonitor::new(Ok(tick1), stream::iter(vec![Ok(tick2)]), config, mock_time_provider);
        monitor.wait_for_qos_ticks_with_timeout(&expected).await.unwrap();

        let status = monitor.get_status().unwrap();
        assert_eq!(status, QosStatus::Healthy);
    }

    #[tokio::test]
    async fn test_get_status_hb_frequency_violation() {
        let now: SystemTime = Timestamp { seconds: 500, nanos: 0, ..Default::default() }.into();
        let mock_time_provider = Box::new(move || now);
        let config = QosConfig {
            latency_threshold: Some(Duration::from_millis(100)),
            frequency_threshold: Some(Duration::from_millis(200)),
        };

        // last tick was 500ms ago, threshold*2 = 400ms. Violation!
        let tick1 = QosTick {
            latency: Duration::from_millis(50),
            received_time: now - Duration::from_millis(700),
        };
        let tick2 = QosTick {
            latency: Duration::from_millis(50),
            received_time: now - Duration::from_millis(500),
        };

        let expected = (Some(Ok(tick1)), Ok(tick2));
        let mut monitor =
            QosMonitor::new(Ok(tick1), stream::iter(vec![Ok(tick2)]), config, mock_time_provider);
        monitor.wait_for_qos_ticks_with_timeout(&expected).await.unwrap();

        let status = monitor.get_status().unwrap();
        assert_eq!(status, QosStatus::UnhealthyHBFrequencyViolation);
    }

    #[tokio::test]
    async fn test_get_status_wire_frequency_violation() {
        let now: SystemTime = Timestamp { seconds: 500, nanos: 0, ..Default::default() }.into();
        let mock_time_provider = Box::new(move || now);

        let config = QosConfig {
            latency_threshold: Some(Duration::from_millis(100)),
            frequency_threshold: Some(Duration::from_millis(200)),
        };

        // interval between tick1 and tick2 is 300ms, threshold = 200ms. Violation!
        let tick1 = QosTick {
            latency: Duration::from_millis(50),
            received_time: now - Duration::from_millis(400),
        };
        let tick2 = QosTick {
            latency: Duration::from_millis(50),
            received_time: now - Duration::from_millis(100),
        };

        let expected = (Some(Ok(tick1)), Ok(tick2));
        let mut monitor =
            QosMonitor::new(Ok(tick1), stream::iter(vec![Ok(tick2)]), config, mock_time_provider);
        monitor.wait_for_qos_ticks_with_timeout(&expected).await.unwrap();

        let status = monitor.get_status().unwrap();
        assert_eq!(status, QosStatus::UnhealthyWireFrequencyViolation);
    }

    #[tokio::test]
    async fn test_get_status_latency_violation() {
        let now: SystemTime = Timestamp { seconds: 500, nanos: 0, ..Default::default() }.into();
        let mock_time_provider = Box::new(move || now);
        let config = QosConfig {
            latency_threshold: Some(Duration::from_millis(100)),
            frequency_threshold: Some(Duration::from_millis(200)),
        };

        // latency is 150ms, threshold = 100ms. Violation!
        let tick1 = QosTick {
            latency: Duration::from_millis(50),
            received_time: now - Duration::from_millis(250),
        };
        let tick2 = QosTick {
            latency: Duration::from_millis(150),
            received_time: now - Duration::from_millis(100),
        };

        let expected = (Some(Ok(tick1)), Ok(tick2));
        let mut monitor =
            QosMonitor::new(Ok(tick1), stream::iter(vec![Ok(tick2)]), config, mock_time_provider);
        monitor.wait_for_qos_ticks_with_timeout(&expected).await.unwrap();

        let status = monitor.get_status().unwrap();
        assert_eq!(status, QosStatus::UnhealthyLatencyViolation);
    }

    #[tokio::test]
    async fn test_get_status_stream_error() {
        let config = QosConfig {
            latency_threshold: Some(Duration::from_millis(100)),
            frequency_threshold: Some(Duration::from_millis(200)),
        };

        let tick = QosTick { latency: Duration::from_millis(50), received_time: SystemTime::now() };

        let expected = (Some(Ok(tick)), Err(SdvStatus::new(SdvStatusCode::Internal)));
        let mut monitor = QosMonitor::new(
            Ok(tick),
            stream::iter(vec![Err(SdvStatus::new(SdvStatusCode::Internal))]),
            config,
            Box::new(SystemTime::now),
        );
        monitor.wait_for_qos_ticks_with_timeout(&expected).await.unwrap();

        let result = monitor.get_status();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status_code(), SdvStatusCode::InvalidArgument);
    }

    #[tokio::test]
    async fn test_dump() {
        let now: SystemTime = Timestamp { seconds: 500, nanos: 0, ..Default::default() }.into();
        let mock_time_provider = Box::new(move || now);

        let config = QosConfig {
            latency_threshold: Some(Duration::from_millis(100)),
            frequency_threshold: Some(Duration::from_millis(200)),
        };
        let tick = QosTick { latency: Duration::from_millis(50), received_time: now };
        let monitor = QosMonitor::new(Ok(tick), stream::pending(), config, mock_time_provider);
        let mut writer = Vec::new();
        monitor.dump(&mut writer, &[]).unwrap();
        let output = String::from_utf8(writer).unwrap();

        assert!(output.contains("MONITOR INTERNAL DATA:"));
        assert!(output.contains("health status: Ok(Healthy)"));
        assert!(output.contains("task running: true"));
    }

    #[test]
    fn test_check_hb_frequency_violation() {
        let threshold = Duration::from_millis(100);
        let now = SystemTime::now();
        let tick =
            QosTick { latency: Duration::ZERO, received_time: now - Duration::from_millis(50) };

        // Case: No violation
        assert!(!check_hb_frequency_violation(Some(threshold), tick, now).unwrap());

        // Case: Violation (threshold * 2 = 200ms, elapsed = 250ms)
        let late_tick =
            QosTick { latency: Duration::ZERO, received_time: now - Duration::from_millis(250) };
        assert!(check_hb_frequency_violation(Some(threshold), late_tick, now).unwrap());

        // Case: threshold is None
        assert!(!check_hb_frequency_violation(None, late_tick, now).unwrap());
    }

    #[test]
    fn test_check_wire_latency_violation() {
        let threshold = Duration::from_millis(100);
        let tick = QosTick { latency: Duration::from_millis(50), received_time: SystemTime::now() };

        // Case: No violation
        assert!(!check_wire_latency_violation(Some(threshold), tick));

        // Case: Violation
        let slow_tick =
            QosTick { latency: Duration::from_millis(150), received_time: SystemTime::now() };
        assert!(check_wire_latency_violation(Some(threshold), slow_tick));

        // Case: threshold is None
        assert!(!check_wire_latency_violation(None, slow_tick));
    }

    #[test]
    fn test_check_wire_frequency_violation() {
        let threshold = Duration::from_millis(100);
        let now = SystemTime::now();
        let prev =
            QosTick { latency: Duration::ZERO, received_time: now - Duration::from_millis(150) };
        let curr = QosTick { latency: Duration::ZERO, received_time: now };

        // Case: Violation (elapsed = 150ms > 100ms)
        assert!(check_wire_frequency_violation(Some(threshold), Some(prev), curr).unwrap());

        // Case: No violation
        let fast_curr =
            QosTick { latency: Duration::ZERO, received_time: now - Duration::from_millis(100) };
        assert!(!check_wire_frequency_violation(Some(threshold), Some(prev), fast_curr).unwrap());

        // Case: threshold is None
        assert!(!check_wire_frequency_violation(None, Some(prev), curr).unwrap());

        // Case: previous_tick is None
        assert!(!check_wire_frequency_violation(Some(threshold), None, curr).unwrap());
    }

    #[test]
    fn test_qos_tick_try_from_valid() {
        let mut hb = QosHeartbeat::new();
        let mut ts_sent = Timestamp::new();
        ts_sent.seconds = 1000;
        ts_sent.nanos = 0;
        hb.data_timestamp = MessageField::some(ts_sent);

        let mut ts_recv = Timestamp::new();
        ts_recv.seconds = 1000;
        ts_recv.nanos = 500_000_000; // 500ms latency
        hb.timestamp = MessageField::some(ts_recv.clone());

        let tick = QosTick::try_from(hb).expect("Failed to convert QosHeartbeat to QosTick");
        assert_eq!(tick.latency, Duration::from_millis(500));
        let expected_received_time: SystemTime = ts_recv.into();
        assert_eq!(tick.received_time, expected_received_time);
    }

    #[test]
    fn test_qos_tick_try_from_invalid_order() {
        let mut hb = QosHeartbeat::new();
        let mut ts_sent = Timestamp::new();
        ts_sent.seconds = 1000;
        ts_sent.nanos = 500_000_000;
        hb.data_timestamp = MessageField::some(ts_sent);

        let mut ts_recv = Timestamp::new();
        ts_recv.seconds = 1000;
        ts_recv.nanos = 0;
        hb.timestamp = MessageField::some(ts_recv);

        let result = QosTick::try_from(hb);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status_code(), SdvStatusCode::InvalidArgument);
    }
}
