//
// db3_signer.rs
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

use crate::db3_address::DB3Address;
use crate::db3_keypair::DB3KeyPair;
use crate::db3_signature::Signature;
use db3_error::{DB3Error, Result};
use ethers::core::types::transaction::eip712::{Eip712, TypedData};
use signature::Signer;

pub struct Db3MultiSchemeSigner {
    kp: DB3KeyPair,
}

impl Db3MultiSchemeSigner {
    pub fn new(kp: DB3KeyPair) -> Self {
        Self { kp }
    }

    // sign msg
    pub fn sign(&self, msg: &[u8]) -> Result<Signature> {
        let signature: Signature = self
            .kp
            .try_sign(msg)
            .map_err(|e| DB3Error::SignMessageError(format!("{e}")))?;
        Ok(signature)
    }

    pub fn sign_typed_data(&self, typed_data: &TypedData) -> Result<Vec<u8>> {
        let hashed = typed_data.encode_eip712().map_err(|e| {
            DB3Error::SignError(format!("fail to generate typed data hash for {e}"))
        })?;
        self.kp.try_sign_hashed_message(&hashed)
    }

    pub fn get_address(&self) -> Result<DB3Address> {
        let pk = self.kp.public();
        Ok(DB3Address::from(&pk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db3_signature::DB3Signature;
    use crate::db3_verifier::DB3Verifier;
    use crate::key_derive;
    use crate::signature_scheme::SignatureScheme;
    use bytes::BytesMut;
    use db3_proto::db3_base_proto::{BroadcastMeta, ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation, PayloadType};
    use ethers::core::types::transaction::eip712::{EIP712Domain, Types};
    use ethers::core::types::Bytes;
    use hex;
    use prost::Message;
    use std::collections::BTreeMap;

    fn db3_signer_smoke_test(scheme: &SignatureScheme) {
        let meta = BroadcastMeta {
            //TODO get from network
            nonce: 1,
            //TODO use config
            chain_id: ChainId::DevNet.into(),
            //TODO use config
            chain_role: ChainRole::StorageShardChain.into(),
        };
        let dm = DatabaseMutation {
            meta: Some(meta),
            collection_mutations: vec![],
            db_address: vec![],
            action: DatabaseAction::CreateDb.into(),
            document_mutations: vec![],
            db_desc: "".to_string(),
        };

        let mut buf = BytesMut::with_capacity(1024 * 8);
        dm.encode(&mut buf)
            .map_err(|e| DB3Error::SignError(format!("{e}")))
            .unwrap();
        let buf = buf.freeze();
        let seed: [u8; 32] = [0; 32];
        let (address, keypair) =
            key_derive::derive_key_pair_from_path(&seed, None, scheme).unwrap();
        let signer = Db3MultiSchemeSigner::new(keypair);
        let signature_ret = signer.sign(&buf);
        assert_eq!(true, signature_ret.is_ok());
        let signature = signature_ret.unwrap();
        let result = signature.verify(&buf);
        assert_eq!(true, result.is_ok());
        assert_eq!(
            serde_json::to_string(&address).unwrap(),
            serde_json::to_string(&result.unwrap()).unwrap()
        );
    }

    fn db3_signer_typed_data(scheme: &SignatureScheme) -> Result<()> {
        let meta = BroadcastMeta {
            //TODO get from network
            nonce: 1,
            //TODO use config
            chain_id: ChainId::DevNet.into(),
            //TODO use config
            chain_role: ChainRole::StorageShardChain.into(),
        };
        let dm = DatabaseMutation {
            meta: Some(meta),
            collection_mutations: vec![],
            db_address: vec![],
            action: DatabaseAction::CreateDb.into(),
            document_mutations: vec![],
            db_desc: "".to_string(),
        };

        let mut payload = BytesMut::with_capacity(1024 * 8);
        dm.encode(&mut payload)
            .map_err(|e| DB3Error::SignError(format!("{e}")))
            .unwrap();
        let payload = payload.freeze();
        let json = serde_json::json!({
          "EIP712Domain": [
          ],
          "Message":[
          {"name":"payload", "type":"bytes"},
          {"name":"payloadType", "type":"string"}
          ]
        });
        let types: Types = serde_json::from_value(json).unwrap();
        let payload: String = format!("{}", Bytes::from(payload));
        assert_eq!(2, types.len());
        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        message.insert(
            "payload".to_string(),
            serde_json::Value::from(payload.as_str()),
        );
        message.insert(
            "payloadType".to_string(),
            serde_json::Value::from(format!("{}", PayloadType::DatabasePayload as i32)),
        );
        let typed_data = TypedData {
            domain: EIP712Domain {
                name: None,
                version: None,
                chain_id: None,
                verifying_contract: None,
                salt: None,
            },
            types,
            primary_type: "Message".to_string(),
            message,
        };
        let seed: [u8; 32] = [2; 32];
        let (address, keypair) =
            key_derive::derive_key_pair_from_path(&seed, None, scheme).unwrap();
        println!("keypair pk {:?}", keypair.public().as_ref());
        let signer = Db3MultiSchemeSigner::new(keypair);
        let signature = signer.sign_typed_data(&typed_data)?;
        let hashed_message: [u8; 32] = typed_data.encode_eip712().unwrap();
        let account_id = DB3Verifier::verify_hashed(&hashed_message, signature.as_ref())?;
        if account_id.addr != address {
            Err(DB3Error::SignMessageError("bad signature".to_string()))
        } else {
            Ok(())
        }
    }

    #[test]
    fn db3_signer_ed25519_typed_data_smoke_test() {
        assert!(db3_signer_typed_data(&SignatureScheme::ED25519).is_err());
    }

    #[test]
    fn db3_signer_secp256k1_typed_data_smoke_test() {
        if !db3_signer_typed_data(&SignatureScheme::Secp256k1).is_ok() {
            assert!(false);
        }
    }

    #[test]
    fn db3_signer_ed25519_smoke_test() {
        db3_signer_smoke_test(&SignatureScheme::ED25519);
    }

    #[test]
    fn db3_signer_secp256k1_smoke_test() {
        db3_signer_smoke_test(&SignatureScheme::Secp256k1);
    }

    #[test]
    fn test_metamask_signature() {
        let signature_hex = "0175fc4a2894184c644b0369a9fbea2425c86ffbd7b60a23d07ce3c25a394ebb05673e5970ad937c5d949f2c34e30c69345e345451fe1f40283cd85d2b00220c9e00032d4ebab1b807ed6b326f88fb44c68e674103a9c39c371592eec75c5f7955f419";
        let hashed_msg_hex = "fee42a65dfa333a4f14c957d51bf69518c241b0e16d0df524a0dc0cdce19cf25";
        let json = serde_json::json!({
          "EIP712Domain": [
          ],
          "Message":[
          {"name":"payload", "type":"bytes"},
          {"name":"payloadType", "type":"string"}
          ]
        });
        let types: Types = serde_json::from_value(json).unwrap();
        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        let payload = "0x48656c6c6f20776f726c6421";
        message.insert(
            "payload".to_string(),
            serde_json::Value::from(payload.to_string()),
        );
        message.insert(
            "payloadType".to_string(),
            serde_json::Value::from("1".to_string()),
        );
        let typed_data = TypedData {
            domain: EIP712Domain {
                name: None,
                version: None,
                chain_id: None,
                verifying_contract: None,
                salt: None,
            },
            types,
            primary_type: "Message".to_string(),
            message,
        };
        let hashed_message: [u8; 32] = typed_data.encode_eip712().unwrap();
        assert_eq!(hex::encode(&hashed_message).as_str(), hashed_msg_hex);
        let signature = hex::decode(signature_hex).unwrap();
        let result = DB3Verifier::verify_hashed(&hashed_message, signature.as_ref());
        assert!(result.is_ok());
        let account = result.unwrap();
        println!("{}", serde_json::to_string(&account.addr).unwrap());
        let result = DB3Verifier::verify_evm_hashed(&hashed_message, signature.as_ref()).unwrap();
        println!("{}", serde_json::to_string(&result.addr).unwrap());
        println!("0x2df74619717c29a7253455e5767f4d992cfb6e3e");
    }
}
