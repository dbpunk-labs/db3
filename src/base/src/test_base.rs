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
use rand::{thread_rng, Rng};

// this function is used for testing
//
pub fn get_a_static_keypair() -> Keypair {
    let secret_key: &[u8] = b"833fe62409237b9d62ec77587520911e9a759cec1d19755b7da901b96dca3d42";
    let public_key: &[u8] = b"ec172b93ad5e563bf4932c70e1245034c35467ef2efd4d64ebf819683467e2bf";
    let sec_bytes: Vec<u8> = FromHex::from_hex(secret_key).unwrap();
    let pub_bytes: Vec<u8> = FromHex::from_hex(public_key).unwrap();
    let secret: SecretKey = SecretKey::from_bytes(&sec_bytes[..SECRET_KEY_LENGTH]).unwrap();
    let public: PublicKey = PublicKey::from_bytes(&pub_bytes[..PUBLIC_KEY_LENGTH]).unwrap();
    Keypair { secret, public }
}
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

pub fn get_a_ts_static_keypair() -> Keypair {
    let secret_key: &[u8] = b"ea82176302fbf6b10a6c7ff25dc77b4b7dee0126841af0fc3621d7ed0ac7c9c99806d5ba5c35c68ff63850fb3f4c5dfc79135c3c2c76a560eeaee6f2135830d6";
    let public_key: &[u8] = b"9806d5ba5c35c68ff63850fb3f4c5dfc79135c3c2c76a560eeaee6f2135830d6";
    let sec_bytes: Vec<u8> = FromHex::from_hex(secret_key).unwrap();
    let pub_bytes: Vec<u8> = FromHex::from_hex(public_key).unwrap();
    let secret: SecretKey = SecretKey::from_bytes(&sec_bytes[..SECRET_KEY_LENGTH]).unwrap();
    let public: PublicKey = PublicKey::from_bytes(&pub_bytes[..PUBLIC_KEY_LENGTH]).unwrap();
    Keypair { secret, public }
}
