//
//
// sign.rs
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

use fastcrypto::secp256k1::Secp256k1KeyPair;
use fastcrypto::traits::KeyPair;
use fastcrypto::traits::Signer;
use rand::rngs::StdRng;
use rand::SeedableRng;
use prost::Message;
pub fn keys() -> Vec<Secp256k1KeyPair> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4)
        .map(|_| Secp256k1KeyPair::generate(&mut rng))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{WriteRequest, Mutation, KvPair};
    use bytes::{Bytes, BytesMut};
    use fastcrypto::secp256k1::Secp256k1Signature;
    use hex;
    #[test]
    fn test_sign() {
		let kp = keys().pop().unwrap();
        let kv = KvPair{
            key:"k1".as_bytes().to_vec(),
            value:"value1".as_bytes().to_vec(),
        };
        let mutation = Mutation {
            ns: "my_twitter".as_bytes().to_vec(),
            kv_pairs:vec![kv],
            nonce:1,
            chain_id:ChainId::MainNet.into(),
            chain_role:ChainRole::StorageShardChain.into(),
        };
        let mut buf = BytesMut::with_capacity(1024 * 4);
        mutation.encode(&mut buf);
        let buf = buf.freeze();
		let signature: Secp256k1Signature = kp.sign(buf.as_ref());
        let request = WriteRequest {
            signature: signature.as_ref().to_vec(),
            mutation:buf.as_ref().to_vec(),
            public_key: kp.public().as_ref().to_vec()
        };
        let mut buf = BytesMut::with_capacity(1024 * 4);
        request.encode(&mut buf);
        let buf = buf.freeze();
        println!("request 0x{}",hex::encode(buf.as_ref()));
		//let recovered_key = signature
		//	.recover(Keccak256::digest(message).as_ref())
		//	.unwrap();
		//assert_eq!(*kp.public(), recovered_key);
    }

}
