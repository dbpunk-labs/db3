//
//
// signer.rs
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

use db3_error::Result;
use ed25519_dalek::{Keypair, Signature, Signer, PUBLIC_KEY_LENGTH, SIGNATURE_LENGTH};

pub struct Db3Signer {
    kp: KeyPair,
}

impl Db3Signer {
    pub fn new(kp: KeyPair) -> Self {
        Self { kp }
    }

    // sign msg
    pub fn sign(&self, msg: &[u8]) -> Result<([u8; SIGNATURE_LENGTH], [u8; PUBLIC_KEY_LENGTH])> {
        let signature: Signature = self.kp.sign(msg);
        Ok((signature.to_bytes(), self.kp.public.to_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::test_base;
    use bytes::BytesMut;
    use db3_error::DB3Error;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::Mutation;
    use db3_proto::db3_mutation_proto::{KvPair, MutationAction};
    use ed25519_dalek::Verifier;
    use prost::Message;
    #[test]
    fn smoke_test() -> Result<()> {
        let kp = test_base::get_a_static_keypair();
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
        mutation
            .encode(&mut buf)
            .map_err(|e| DB3Error::SignError(format!("{}", e)))?;
        let buf = buf.freeze();
        let signer = Db3Signer::new(kp);
        let result = signer.sign(buf.as_ref());
        assert!(result.is_ok());
        let signature = Signature::try_from(result.unwrap()).unwrap();
        let the_same_kp = test_base::get_a_static_keypair();
        let result = the_same_kp.verify(buf.as_ref(), &signature);
        assert!(result.is_ok());
        Ok(())
    }
}
