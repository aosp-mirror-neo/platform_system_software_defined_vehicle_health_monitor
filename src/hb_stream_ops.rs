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
    s.pipe(observe::<ServiceHeartbeat>).into_flattened_heartbeats().with_monitoring_start_event()
}

trait HertbeatVecStream {
    fn into_flattened_heartbeats(self) -> impl Stream<Item = SdvResult<HeartbeatEvent>> + Send;
}

impl<T: Stream<Item = SdvResult<Vec<ServiceHeartbeat>>> + Send> HertbeatVecStream for T {
    fn into_flattened_heartbeats(self) -> impl Stream<Item = SdvResult<HeartbeatEvent>> + Send {
        self.map(|res| match res {
            Ok(v) => v.into_iter().map(TryInto::try_into).pipe(stream::iter).left_stream(),
            Err(e) => stream::once(async { Err(e) }).right_stream(),
        })
        .flatten()
    }
}

trait HeartbeatStream {
    fn with_monitoring_start_event(self) -> impl Stream<Item = SdvResult<HeartbeatEvent>> + Send;
}

impl<T: Stream<Item = SdvResult<HeartbeatEvent>> + Send> HeartbeatStream for T {
    fn with_monitoring_start_event(self) -> impl Stream<Item = SdvResult<HeartbeatEvent>> + Send {
        stream::once(future::ready(Ok(HeartbeatEvent::TrackingHeartbeat(
            crate::hb_monitor::HeartbeatTrackEvent::StartedMonitoring(SystemTime::now()),
        ))))
        .chain(self)
    }
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
        let output_stream = input_stream.into_flattened_heartbeats();
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
        let output_stream = input_stream.into_flattened_heartbeats();
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
