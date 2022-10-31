//
// test_base.rs
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

use super::get_address_from_pk;
use ethereum_types::Address as AccountAddress;
use fastcrypto::secp256k1::Secp256k1PublicKey;
use fastcrypto::traits::ToFromBytes;
use hex;
use rust_secp256k1::PublicKey;

pub fn get_static_pk() -> PublicKey {
    let pk = Secp256k1PublicKey::from_bytes(
        &hex::decode("03ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138").unwrap(),
    )
    .unwrap();
    pk.pubkey
}

pub fn get_a_static_address() -> AccountAddress {
    let pk = get_static_pk();
    get_address_from_pk(&pk)
}
