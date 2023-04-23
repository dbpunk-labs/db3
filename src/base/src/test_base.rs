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
use ed25519_dalek::{Keypair, PublicKey, SecretKey, PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH};
use ethereum_types::Address as AccountAddress;
use hex::FromHex;
use rand::Rng;

// this function is used for testing
pub fn get_a_random_nonce() -> u64 {
    let mut rng = rand::thread_rng();
    let nonce = rng.gen_range(0..100000000);
    nonce
}

pub fn get_a_static_address() -> AccountAddress {
    let kp = get_a_static_keypair();
    get_address_from_pk(&kp.public)
}
