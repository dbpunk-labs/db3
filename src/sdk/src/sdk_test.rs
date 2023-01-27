//
// sdk_test.rs
// Copyright (C) 2023 db3.network Author imotai <codego.me@gmail.com>
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

use db3_crypto::{
    db3_address::DB3Address, db3_keypair::DB3KeyPair, db3_signer::Db3MultiSchemeSigner, key_derive,
    signature_scheme::SignatureScheme,
};

pub fn gen_ed25519_signer() -> Db3MultiSchemeSigner {
    let seed: [u8; 32] = [0; 32];
    let (_, kp) =
        key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::ED25519).unwrap();
    Db3MultiSchemeSigner::new(kp)
}

pub fn gen_secp256k1_signer() -> Db3MultiSchemeSigner {
    let seed: [u8; 32] = [0; 32];
    let (_, kp) =
        key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::Secp256k1).unwrap();
    Db3MultiSchemeSigner::new(kp)
}
