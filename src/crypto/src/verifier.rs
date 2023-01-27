//
//
// verifier.rs
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

use super::account_id::AccountId;
use db3_error::{DB3Error, Result};
use ed25519_dalek::{PublicKey, Signature, Verifier as EdVerifier};

pub struct Verifier {}

impl Verifier {
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signer::Db3Signer;
    use bytes::BytesMut;
    use db3_base::get_a_static_keypair;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction};
    use prost::Message;
    #[test]
    fn test_verify() -> Result<()> {
        let kp = get_a_static_keypair();
        let kv = KvPair {
            key: "k1".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let mutation = Mutation {
            ns: "my_twitter".as_bytes().to_vec(),
            kv_pairs: vec![kv],
            nonce: 1,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            gas_price: None,
            gas: 10,
        };
        let mut buf = BytesMut::with_capacity(1024 * 8);
        mutation.encode(&mut buf).unwrap();
        let buf = buf.freeze();
        let signer = Db3Signer::new(kp);
        let (signature_raw, public_key_raw) = signer.sign(buf.as_ref())?;
        if let Err(_) = Verifier::verify(
            buf.as_ref(),
            signature_raw.as_ref(),
            public_key_raw.as_ref(),
        ) {
            assert!(false);
        }
        Ok(())
    }
}
