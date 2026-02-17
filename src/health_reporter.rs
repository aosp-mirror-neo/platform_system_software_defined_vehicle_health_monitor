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

use health_api::health_topic::VmHealth;
use log::warn;
use mockall::automock;
use protobuf::Message;
use sdv_mw_rs_comms::Publisher;
use sdv_status_rs::SdvResult;
use tracing::instrument;

pub struct HealthReporter {
    vm_health_publisher: Box<dyn MessagePublisher<VmHealth>>,
}

impl HealthReporter {
    pub fn new(vm_health_publisher: Box<dyn MessagePublisher<VmHealth>>) -> Self {
        Self { vm_health_publisher }
    }

    #[instrument(skip(self))]
    pub fn report_vm_health(
        &self,
        hbs_missed: bool,
        recoveries_failed: bool,
        qos_violations_detected: bool,
    ) -> SdvResult<()> {
        // TODO rework VMHealth output API to take into account agent monitoring,
        // and provide full observability into monitoring state
        let vm_health = VmHealth {
            all_monitored_service_bundles_healthy: !hbs_missed,
            all_service_bundles_alive: !recoveries_failed,
            qos_violations_detected,
            ..Default::default()
        };
        self.vm_health_publisher.publish(&vm_health)
    }
}

#[automock]
pub trait MessagePublisher<M: Message>: Send {
    fn publish(&self, data: &M) -> SdvResult<()>;
}

impl<M: Message> MessagePublisher<M> for Publisher<M> {
    fn publish(&self, data: &M) -> SdvResult<()> {
        Publisher::publish(self, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sdv_status_rs::SdvStatus;
    use sdv_status_rs::SdvStatusCode;

    #[test]
    fn report_vm_health_returns_error_when_publishing_fails() {
        let mut mock_publisher = MockMessagePublisher::<VmHealth>::new();
        mock_publisher
            .expect_publish()
            .returning(|_| Err(SdvStatus::with_message(SdvStatusCode::Internal, "Publish failed")));

        let reporter = HealthReporter::new(Box::new(mock_publisher));
        let result = reporter.report_vm_health(true, true, true);

        assert!(result.is_err());
    }
}
