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

use std::collections::HashMap;
use std::hash::Hash;
use std::mem;

/// adds to a HashMap, avoiding copy of key unless absolutely required.
/// Does not take ownership of K.
/// Returns previous value from entry of map, if existed.
pub fn insert<K, V>(map: &mut HashMap<K, V>, key: &K, value: V) -> Option<V>
where
    K: Hash,
    K: Clone,
    K: std::cmp::Eq,
{
    if let Some(old) = map.get_mut(key) {
        Some(mem::replace(old, value))
    } else {
        map.insert(key.clone(), value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insertion_inserts_and_returns_old_value() {
        let mut map = HashMap::<usize, usize>::from([(0, 44)]);
        let old = insert(&mut map, &0, 123);
        assert!(old == Some(44));
        assert!(map.get(&0) == Some(&123));
    }
}
