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
use sdv_comms::id::ServiceFqin;

pub struct AuthorizationImpl {
    fqin: ServiceFqin,
}

impl AuthorizationImpl {
    pub fn new(fqin: ServiceFqin) -> Self {
        Self { fqin }
    }

    pub fn is_request_authorized(&self, fqin: &ServiceFqin) -> bool {
        self.fqin.get_sdv_vm_name() == fqin.get_sdv_vm_name()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_request_authorized_returns_true_for_same_vm() {
        let fqin = ServiceFqin::builder()
            .sdv_vm_name("vm")
            .sdv_package_name("com.google.sdv")
            .service_bundle_name("HealthyServiceBundle")
            .service_instance_name("instance1")
            .build()
            .unwrap();
        let auth = AuthorizationImpl::new(fqin);

        let client_fqin = ServiceFqin::builder()
            .sdv_vm_name("vm")
            .sdv_package_name("com.oem.package")
            .service_bundle_name("HealthyServiceBundle")
            .service_instance_name("instance2")
            .build()
            .unwrap();

        assert!(auth.is_request_authorized(&client_fqin));
    }

    #[test]
    fn is_request_authorized_returns_false_for_different_vm() {
        let fqin = ServiceFqin::builder()
            .sdv_vm_name("vm1")
            .sdv_package_name("com.google.sdv")
            .service_bundle_name("HealthyServiceBundle")
            .service_instance_name("instance1")
            .build()
            .unwrap();
        let auth = AuthorizationImpl::new(fqin);

        let client_fqin = ServiceFqin::builder()
            .sdv_vm_name("vm2")
            .sdv_package_name("com.google.sdv")
            .service_bundle_name("HealthyServiceBundle")
            .service_instance_name("instance1")
            .build()
            .unwrap();

        assert!(!auth.is_request_authorized(&client_fqin));
    }
}
