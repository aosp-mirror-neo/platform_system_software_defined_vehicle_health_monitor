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

use crate::commstack_trait::Comms;
use crate::hb_monitor::HeartbeatEvent;
use futures::stream;
use futures::Stream;
use futures::StreamExt;
use health_api::health_topic::ServiceHeartbeat;
use sdv_comms::dt::SubscriberOptions;
use sdv_mw_rs_comms::{observe, ByteBuffersStream};
use sdv_status_rs::SdvResult;
use sdv_status_rs::SdvStatus;
use std::future;
use std::future::Future;
use std::time::Duration;
use tap::prelude::*;

/// Constructs stream of heartbeats from a HB sud future *synchronously*: the initial future
/// is polled by the consumer of the constructed stream
pub fn hb_sud_id_to_hb_stream<Hb: HeartbeatType, T: Comms>(
    context: T::ContextRef,
    hb_sud_id: impl Future<Output = SdvResult<T::UnitIdentifier>> + Send,
) -> impl Stream<Item = SdvResult<Hb>> + Send {
    stream::once(hb_sud_id)
        .then(move |res| async move {
            match res {
                Ok(id) => {
                    T::create_untyped_stream(
                        context,
                        id,
                        Duration::ZERO,
                        SubscriberOptions { fetch_last_message: true },
                    )
                    .await
                }
                Err(e) => Err(e),
            }
        })
        .map(|res| match res {
            Ok(s) => s.pipe(Box::pin).pipe(|s| into_heartbeats(s)).left_stream(),
            Err(e) => stream::once(async { Err(e) }).right_stream(),
        })
        .flatten()
}

fn into_heartbeats<T: HeartbeatType>(
    s: ByteBuffersStream,
) -> impl Stream<Item = SdvResult<T>> + Send {
    s.pipe(observe::<T::WireType>).pipe(into_flattened_heartbeats)
}

fn into_flattened_heartbeats<T: HeartbeatType>(
    input: impl Stream<Item = SdvResult<Vec<T::WireType>>> + Send,
) -> impl Stream<Item = SdvResult<T>> + Send {
    input
        .map(|res| match res {
            Ok(v) => v.into_iter().map(TryInto::try_into).pipe(stream::iter).left_stream(),
            Err(e) => stream::once(async { Err(e) }).right_stream(),
        })
        .flatten()
}

pub fn with_monitoring_start_event<T: HeartbeatType>(
    start_event: T,
    input: impl Stream<Item = SdvResult<T>> + Send,
) -> impl Stream<Item = SdvResult<T>> + Send {
    stream::once(future::ready(Ok(start_event))).chain(input)
}

pub trait HeartbeatType: TryFrom<Self::WireType, Error = SdvStatus> + Send {
    type WireType: protobuf::Message;
}

impl HeartbeatType for HeartbeatEvent {
    type WireType = ServiceHeartbeat;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hb_monitor::HeartbeatTrackEvent;
    use health_api::health_topic::ServiceHeartbeat;
    use protobuf::well_known_types::timestamp::Timestamp;
    use protobuf::Message;
    use protobuf::MessageField;
    use sdv_status_rs::SdvStatus;
    use std::pin::pin;
    use std::time::SystemTime;
    use tokio::time;

    #[tokio::test]
    async fn test_into_flattened_heartbeats_flattens_and_converts() {
        let input_stream = stream::once(future::ready(SdvResult::Ok(vec![
            ServiceHeartbeat::default(),
            ServiceHeartbeat::default(),
        ])));
        let output_stream = input_stream.pipe(into_flattened_heartbeats::<HeartbeatEvent>);
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
        let output_stream = input_stream.pipe(into_flattened_heartbeats::<HeartbeatEvent>);
        let mut output_stream = pin!(output_stream);

        let res1 = output_stream.next().await.expect("Expecting 1 heartbeat");
        assert!(res1.is_err());
        assert!(output_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_into_heartbeats_propagates_errors() {
        let input_stream: ByteBuffersStream = Box::pin(stream::once(future::ready(
            SdvResult::Err(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound)),
        )));

        let output_stream = into_heartbeats::<HeartbeatEvent>(input_stream).pipe(|s| {
            with_monitoring_start_event(
                HeartbeatEvent::TrackingHeartbeat(HeartbeatTrackEvent::StartedMonitoring(
                    SystemTime::now(),
                )),
                s,
            )
        });
        let mut output_stream = pin!(output_stream);

        let _res1 = output_stream.next().await.expect("Expecting 1 event");
        let res2 = output_stream.next().await.expect("Expecting 2 events");
        assert!(res2.is_err());
    }

    #[tokio::test]
    async fn test_hb_stream_valid_when_sud_return_succeeds_and_dt_stream_creation_succeeds() {
        //SETUP
        const HB_TIMESTAMP_S: i64 = 33i64;
        const HB_TIMESTAMP_NS: i32 = 44i32;

        struct TestableComms;
        impl Comms for TestableComms {
            type ContextRef = ();
            type UnitIdentifier = ();
            type ServiceUnitDefinition = ();
            type ServiceDiscoveryManager = ();

            /// creates stream of one Heartbeat (mocking case where dt stream is created successfully)
            async fn create_untyped_stream(
                _context: Self::ContextRef,
                _id: Self::UnitIdentifier,
                _duration: Duration,
                _options: SubscriberOptions,
            ) -> SdvResult<impl Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send + 'static>
            {
                let hb = ServiceHeartbeat {
                    timestamp: MessageField::some(Timestamp {
                        seconds: HB_TIMESTAMP_S,
                        nanos: HB_TIMESTAMP_NS,
                        ..Default::default()
                    }),
                    ..Default::default()
                };
                let wire = hb.write_length_delimited_to_bytes().unwrap();

                let nr_of_heartbeats = 1u32;
                (nr_of_heartbeats, wire).pipe(Ok).pipe(future::ready).pipe(stream::once).pipe(Ok)
            }
        }
        let sud_future_returning_valid_sud = future::ready(Ok(()));

        // ACT
        let stream = hb_sud_id_to_hb_stream::<HeartbeatEvent, TestableComms>(
            (),
            sud_future_returning_valid_sud,
        );
        tokio::pin!(stream);
        let res = stream
            .next()
            .pipe(|f| time::timeout(Duration::from_millis(300), f))
            .await
            .unwrap()
            .unwrap();

        // ASSERT: stream returns an event, with the heartbeat received from HB publication
        let res = if let Ok(HeartbeatEvent::TrackingHeartbeat(
            HeartbeatTrackEvent::ReceivedHeartbeat(res),
        )) = res
        {
            res
        } else {
            panic!("stream did not return what we expected");
        };

        let expected =
            Timestamp { seconds: HB_TIMESTAMP_S, nanos: HB_TIMESTAMP_NS, ..Default::default() };
        let expected: SystemTime = Into::into(expected);
        assert!(res == expected);
    }

    #[tokio::test]
    async fn test_hb_stream_produces_errors_when_sud_return_succeeds_but_dt_stream_creation_fails()
    {
        // SETUP
        struct CommsDtStreamCreationError;
        impl Comms for CommsDtStreamCreationError {
            type ContextRef = ();
            type UnitIdentifier = ();
            type ServiceUnitDefinition = ();
            type ServiceDiscoveryManager = ();

            async fn create_untyped_stream(
                _context: Self::ContextRef,
                _id: Self::UnitIdentifier,
                _duration: Duration,
                _options: SubscriberOptions,
            ) -> SdvResult<impl Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send + 'static>
            {
                Err::<stream::Pending<_>, _>(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound))
            }
        }
        let sud_future_returning_valid_sud = future::ready(Ok(()));

        // ACT
        let stream = hb_sud_id_to_hb_stream::<HeartbeatEvent, CommsDtStreamCreationError>(
            (),
            sud_future_returning_valid_sud,
        );
        tokio::pin!(stream);
        let res = stream.next().await;

        // ASSERT: stream produces an error, although sud was retrieved, as create raw dt stream failed
        assert!(res.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_hb_stream_produces_error_when_sud_return_fails() {
        // SETUP
        struct CommsDtStreamCreationError;
        impl Comms for CommsDtStreamCreationError {
            type ContextRef = ();
            type UnitIdentifier = ();
            type ServiceUnitDefinition = ();
            type ServiceDiscoveryManager = ();

            async fn create_untyped_stream(
                _context: Self::ContextRef,
                _id: Self::UnitIdentifier,
                _duration: Duration,
                _options: SubscriberOptions,
            ) -> SdvResult<impl Stream<Item = SdvResult<(u32, Vec<u8>)>> + Send + 'static>
            {
                Err::<stream::Pending<_>, _>(SdvStatus::new(sdv_status_rs::SdvStatusCode::NotFound))
            }
        }
        let sud_future_returning_error = future::ready(Err(SdvStatus::with_message(
            sdv_status_rs::SdvStatusCode::DeadlineExceeded,
            "Error due to timeout waiting for SUD to be registered, for example",
        )));

        // ACT
        let stream = hb_sud_id_to_hb_stream::<HeartbeatEvent, CommsDtStreamCreationError>(
            (),
            sud_future_returning_error,
        );
        tokio::pin!(stream);
        let res = stream.next().await;

        // ASSERT: stream produces an error: sud retrieval failed
        assert!(res.unwrap().is_err());
    }
}
