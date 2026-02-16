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

pub enum QosStatus {
    Healthy,
    UnhealthyLogicError,
    UnhealthyLatencyViolation,
    UnhealthyWireFrequencyViolation,
    UnhealthyHBFrequencyViolation,
}

pub struct QosMonitor {
    listening_task: tokio::task::JoinHandle<()>,
    latest_ticks: watch::Receiver<SdvResult<(QosTick, QosTick)>>,
    config: QosConfig,
}

impl QosMonitor {
    pub fn new(
        qos_stream: impl Stream<Item = SdvResult<(QosTick, QosTick)>> + Send + 'static,
        qos_config: QosConfig,
    ) -> Self {
        let (tx, rx) = tokio::sync::watch::channel(Err(SdvStatus::new(SdvStatusCode::NotFound)));
        let listening_task = tokio::spawn(async move {
            qos_stream
                .for_each(async |t| {
                    let _ = tx.send_replace(t);
                })
                .await;
        });
        Self { listening_task, latest_ticks: rx, config: qos_config }
    }

    fn get_status(&self) -> QosStatus {
        todo!()
    }
}

impl Drop for QosMonitor {
    fn drop(&mut self) {
        self.listening_task.abort();
    }
}

impl DumpStateReporter for QosMonitor {
    fn dump(
        &self,
        _writer: &mut dyn std::io::Write,
        _args: &[&std::ffi::CStr],
    ) -> Result<(), std::io::Error> {
        todo!()
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
    use super::*;
    use futures::stream;
    use protobuf::well_known_types::timestamp::Timestamp;
    use protobuf::MessageField;
    use std::time::Duration;

    #[tokio::test]
    #[should_panic(expected = "not yet implemented")]
    async fn test_get_status_panics() {
        let config = QosConfig {
            latency_threshold: Some(Duration::from_millis(100)),
            frequency_threshold: Some(Duration::from_millis(200)),
        };
        let monitor = QosMonitor::new(stream::pending(), config);
        let _ = monitor.get_status();
    }

    #[tokio::test]
    #[should_panic(expected = "not yet implemented")]
    async fn test_dump_panics() {
        let config = QosConfig {
            latency_threshold: Some(Duration::from_millis(100)),
            frequency_threshold: Some(Duration::from_millis(200)),
        };
        let monitor = QosMonitor::new(stream::pending(), config);
        let mut writer = Vec::new();
        let _ = monitor.dump(&mut writer, &[]);
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
