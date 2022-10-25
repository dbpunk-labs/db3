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

use bytes::BytesMut;
use db3_error::Result;
use db3_proto::db3_mutation_proto::{Mutation, WriteRequest};
use fastcrypto::secp256k1::{Secp256k1KeyPair, Secp256k1Signature};

use fastcrypto::traits::KeyPair;
use fastcrypto::traits::Signer;
use prost::Message;

pub struct MutationSigner {
    kp: Secp256k1KeyPair,
}

//
// Mutationsigner is used in sdk
//
impl MutationSigner {
    pub fn new(kp: Secp256k1KeyPair) -> Self {
        Self { kp }
    }

    // sign mutation
    pub fn sign(&self, mutation: &Mutation) -> Result<WriteRequest> {
        let mut buf = BytesMut::with_capacity(1024 * 8);
        mutation.encode(&mut buf);
        let buf = buf.freeze();
        let signature: Secp256k1Signature = self.kp.sign(buf.as_ref());
        let request = WriteRequest {
            signature: signature.as_ref().to_vec(),
            mutation: buf.to_vec(),
            public_key: self.kp.public().as_ref().to_vec(),
        };
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::KvPair;
    use fastcrypto::secp256k1::Secp256k1PublicKey;
    use fastcrypto::hash::Keccak256;
    use fastcrypto::traits::ToFromBytes;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use fastcrypto::Verifier;
    #[test]
    fn test_sign() -> Result<()> {
        let mut rng = StdRng::from_seed([0; 32]);
        let kp = Secp256k1KeyPair::generate(&mut rng);
        let kv = KvPair {
            key: "k1".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
        };
        let mutation = Mutation {
            ns: "my_twitter".as_bytes().to_vec(),
            kv_pairs: vec![kv],
            nonce: 1,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            gas_price: 1,
            start_gas: 10,
        };
        let signer = MutationSigner::new(kp);
        let request = signer.sign(&mutation)?;
        let signature =
            <Secp256k1Signature as ToFromBytes>::from_bytes(request.signature.as_ref()).unwrap();
        let public_key =
            <Secp256k1PublicKey as ToFromBytes>::from_bytes(request.public_key.as_ref()).unwrap();
        if let Err(_) = public_key.verify(request.mutation.as_ref(), &signature) {
            assert!(false)
        }
        Ok(())
    }
}
