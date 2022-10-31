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
#[cfg(test)]
use super::signer::MutationSigner;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_proto::WriteRequest;
use fastcrypto::secp256k1::Secp256k1Signature;
use fastcrypto::traits::ToFromBytes;
use rust_secp256k1::Message;

pub struct MutationVerifier {}

impl MutationVerifier {
    pub fn verify(request: &WriteRequest) -> Result<AccountId> {
        let signature = <Secp256k1Signature as ToFromBytes>::from_bytes(request.signature.as_ref())
            .map_err(|e| DB3Error::VerifyFailed(format!("{}", e)))?;
        let message = Message::from_hashed_data::<rust_secp256k1::hashes::sha256::Hash>(
            request.mutation.as_ref(),
        );
        if let Ok(rpk) = signature.sig.recover(&message) {
            Ok(AccountId::new(rpk))
        } else {
            Err(DB3Error::VerifyFailed("invalid signature".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{KvPair, Mutation, MutationAction};
    use fastcrypto::secp256k1::Secp256k1KeyPair;
    use fastcrypto::traits::KeyPair;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    #[test]
    fn test_verify() -> Result<()> {
        let mut rng = StdRng::from_seed([0; 32]);
        let kp = Secp256k1KeyPair::generate(&mut rng);
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
        let signer = MutationSigner::new(kp);
        let request = signer.sign(&mutation)?;
        let account_id = MutationVerifier::verify(&request);
        if let Err(e) = account_id {
            println!("{}", e);
            assert!(false);
        }
        Ok(())
    }
}
