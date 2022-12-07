//
// get_address.rs
// Copyright (C) 2022 db3.network Author imotai <codego.me@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use ed25519_dalek::PublicKey;
use ethereum_types::Address;
use fastcrypto::hash::HashFunction;
use fastcrypto::hash::Keccak256;

pub fn get_address_from_pk(pk: &PublicKey) -> Address {
    let hash = Keccak256::digest(&pk.to_bytes()[1..]);
    Address::from_slice(&hash.as_ref()[12..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_base;
    use std::str::FromStr;
    #[test]
    fn test_get_ts_address() {
        let kp = test_base::get_a_ts_static_keypair();
        assert_eq!(
            "0xfab8a023ba3939b232dfde57f4faaf0389fc6c58",
            format!("{:?}", get_address_from_pk(&kp.public))
        );
    }
    #[test]
    fn test_get_address_from_pk() {
        let kp = test_base::get_a_static_keypair();
        assert_eq!(
            "0xffddb10906d3602e7b2059b38034899f70318e17",
            format!("{:?}", get_address_from_pk(&kp.public))
        );
        let addr = Address::from_str("0xffddb10906d3602e7b2059b38034899f70318e17");
        assert_eq!(addr.unwrap(), get_address_from_pk(&kp.public));
    }
}
