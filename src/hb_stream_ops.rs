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

use crate::hb_monitor::HeartbeatEvent;
use futures::stream;
use futures::Stream;
use futures::StreamExt;
use health_api::health_topic::ServiceHeartbeat;
use sdv_mw_rs_comms::{observe, ByteBuffersStream};
use sdv_status_rs::SdvResult;
use std::future;
use std::time::SystemTime;
use tap::prelude::*;

pub fn into_heartbeats(
    s: ByteBuffersStream,
) -> impl Stream<Item = SdvResult<HeartbeatEvent>> + Send {
    s.pipe(observe::<ServiceHeartbeat>)
        .pipe(into_flattened_heartbeats)
        .pipe(with_monitoring_start_event)
}

fn into_flattened_heartbeats(
    input: impl Stream<Item = SdvResult<Vec<ServiceHeartbeat>>> + Send,
) -> impl Stream<Item = SdvResult<HeartbeatEvent>> + Send {
    input
        .map(|res| match res {
            Ok(v) => v.into_iter().map(TryInto::try_into).pipe(stream::iter).left_stream(),
            Err(e) => stream::once(async { Err(e) }).right_stream(),
        })
        .flatten()
}

fn with_monitoring_start_event(
    input: impl Stream<Item = SdvResult<HeartbeatEvent>> + Send,
) -> impl Stream<Item = SdvResult<HeartbeatEvent>> + Send {
    stream::once(future::ready(Ok(HeartbeatEvent::TrackingHeartbeat(
        crate::hb_monitor::HeartbeatTrackEvent::StartedMonitoring(SystemTime::now()),
    ))))
    .chain(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hb_monitor::HeartbeatTrackEvent;
    use health_api::health_topic::ServiceHeartbeat;
    use sdv_status_rs::SdvStatus;
    use std::pin::pin;

    #[tokio::test]
    async fn test_into_flattened_heartbeats_flattens_and_converts() {
        let input_stream = stream::once(future::ready(SdvResult::Ok(vec![
            ServiceHeartbeat::default(),
            ServiceHeartbeat::default(),
        ])));
        let output_stream = input_stream.pipe(into_flattened_heartbeats);
        let mut output_stream = pin!(output_stream);
        let _res1 = output_stream.next().await.expect("Expecting 1 heartbeat");
        let _res2 = output_stream.next().await.expect("Expecting 2 heartbeats");
        assert!(output_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_into_flattened_heartbeats_propagates_errors() {
        let input_stream = stream::once(future::ready(SdvResult::Err(SdvStatus::new(
            sdv_status_rs::SdvStatusCode::NotFound,
        ))));
        let output_stream = input_stream.pipe(into_flattened_heartbeats);
        let mut output_stream = pin!(output_stream);

        let res1 = output_stream.next().await.expect("Expecting 1 heartbeat");
        assert!(res1.is_err());
        assert!(output_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_into_heartbeats_adds_monitoring_start_event() {
        let input_stream: ByteBuffersStream = Box::pin(stream::empty());

        let output_stream = into_heartbeats(input_stream);
        let mut output_stream = pin!(output_stream);

        let res = output_stream.next().await.expect("Expecting 1 heartbeat");
        assert!(matches!(
            res,
            Ok(HeartbeatEvent::TrackingHeartbeat(HeartbeatTrackEvent::StartedMonitoring(_)))
        ));
    }

    #[tokio::test]
    async fn test_into_heartbeats_propagates_errors() {
        let input_stream: ByteBuffersStream = Box::pin(stream::once(future::ready(
            SdvResult::Err(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound)),
        )));

        let output_stream = into_heartbeats(input_stream);
        let mut output_stream = pin!(output_stream);

        let _res1 = output_stream.next().await.expect("Expecting 1 event");
        let res2 = output_stream.next().await.expect("Expecting 2 events");
        assert!(res2.is_err());
    }
}
