<!--
Copyright (C) 2026 The Android Open Source Project

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Monitored Service

This service bundle simulates a monitored service for health monitoring
integration tests. It publishes heartbeat messages and its behavior can be
controlled via commands sent through standard input (stdin). This allows testing
the health monitor's ability to detect healthy and unhealthy states.

## How to use the service

Run the service from your terminal:

```bash
adb shell sdv_integration_test_health_monitored_service
```

The service will start and wait for commands on stdin.

### Supported Commands

The monitored service accepts commands via standard input
(stdin). These commands control its behavior, specifically related to health
status reporting. Here are the four supported commands:

#### RegisterConfiguration

This command registers the previously provided health configuration with the
health monitor. This makes the health monitor aware of the service and its
expected health behavior.

```bash
"RegisterConfiguration"
```

#### DeregisterConfiguration

This command deregisters the health configuration, essentially removing the
service from the health monitor's list of monitored services.

```bash
"DeregisterConfiguration"
```

#### Start Heartbeats Publishing

This command initiates the process of sending heartbeat messages according to
the configured parameters.

```bash
"StartHeartbeatsPublishing"
```

#### Stop Heartbeats Publishing

This command stops the sending of heartbeat messages. This can be used to
simulate an unhealthy state (e.g., the service has crashed or become
unresponsive).

```bash
"StopHeartbeatsPublishing"
```