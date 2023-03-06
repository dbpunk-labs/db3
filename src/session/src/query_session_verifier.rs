//
// query_session_verifier.rs
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

use db3_crypto::db3_verifier;
use db3_crypto::id::AccountId;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_proto::PayloadType;
use db3_proto::db3_session_proto::QuerySessionInfo;
use ethers::types::{
    transaction::eip712::{Eip712, TypedData},
    Bytes,
};
use prost::Message;
use std::str::FromStr;

fn decode_query_session_info(payload: &[u8]) -> Result<QuerySessionInfo> {
    match QuerySessionInfo::decode(payload) {
        Ok(qi) => match &qi.meta {
            Some(_) => Ok(qi),
            None => Err(DB3Error::QuerySessionVerifyError(
                "meta is none".to_string(),
            )),
        },
        Err(_) => Err(DB3Error::QuerySessionVerifyError(
            "invalid mutation data".to_string(),
        )),
    }
}

///
///
/// verify the query session and return the client account id
///
///
pub fn verify_query_session(
    payload: &[u8],
    payload_type: i32,
    signature: &[u8],
) -> Result<(QuerySessionInfo, AccountId)> {
    // typeddata
    if payload_type as i32 == 3 {
        match serde_json::from_slice::<TypedData>(payload) {
            Ok(data) => {
                let hashed_message: [u8; 32] = data.encode_eip712().map_err(|e| {
                    DB3Error::QuerySessionVerifyError(format!("invalid payload type for err {e}"))
                })?;

                let account_id =
                    db3_verifier::DB3Verifier::verify_hashed(&hashed_message, signature)?;

                if let (Some(payload), Some(internal_data_type)) =
                    (data.message.get("payload"), data.message.get("payloadType"))
                {
                    let data: Bytes = serde_json::from_value(payload.clone()).map_err(|e| {
                        DB3Error::QuerySessionVerifyError(format!(
                            "invalid payload type for err {e}"
                        ))
                    })?;
                    let internal_data_type = i32::from_str(internal_data_type.as_str().ok_or(
                        DB3Error::QuerySessionVerifyError("invalid payload type".to_string()),
                    )?)
                    .map_err(|e| {
                        DB3Error::QuerySessionVerifyError(format!(
                            "fail to convert payload type to i32 {e}"
                        ))
                    })?;
                    if internal_data_type != PayloadType::QuerySessionPayload as i32 {
                        return Err(DB3Error::QuerySessionVerifyError(
                            "invalid payload type and query session payload expected".to_string(),
                        ));
                    }
                    Ok((decode_query_session_info(data.as_ref())?, account_id))
                } else {
                    Err(DB3Error::QuerySessionVerifyError(
                        "bad typed data".to_string(),
                    ))
                }
            }
            Err(e) => Err(DB3Error::QuerySessionVerifyError(format!(
                "invalid payload type for err {e}"
            ))),
        }
    } else {
        let account_id = db3_verifier::DB3Verifier::verify(payload, signature)?;
        Ok((decode_query_session_info(payload)?, account_id))
    }
}

pub fn check_query_session_info(
    node_query_session: &QuerySessionInfo,
    client_query_session: &QuerySessionInfo,
) -> bool {
    node_query_session.query_count == client_query_session.query_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use chrono::Utc;
    use db3_crypto::db3_signer::Db3MultiSchemeSigner;
    use db3_crypto::{db3_keypair::DB3KeyPair, key_derive, signature_scheme::SignatureScheme};
    use db3_proto::db3_base_proto::{BroadcastMeta, ChainId, ChainRole};
    use ethers::types::transaction::eip712::EIP712Domain;
    use ethers::types::transaction::eip712::Types;
    use std::collections::BTreeMap;

    fn get_a_static_keypair() -> DB3KeyPair {
        let seed: [u8; 32] = [0; 32];
        let (_, keypair) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::Secp256k1)
                .unwrap();
        keypair
    }

    #[test]
    fn test_verify_typed_data_happy_path() -> Result<()> {
        let meta = BroadcastMeta {
            //TODO get from network
            nonce: 10,
            //TODO use config
            chain_id: ChainId::DevNet.into(),
            //TODO use config
            chain_role: ChainRole::StorageShardChain.into(),
        };
        // the client query session
        let query_session_info = QuerySessionInfo {
            id: 1,
            start_time: Utc::now().timestamp(),
            query_count: 10,
            meta: Some(meta),
        };
        let mut buf = BytesMut::with_capacity(1024 * 8);
        query_session_info.encode(&mut buf).unwrap();
        let payload_session_info = Bytes(buf.freeze());
        let json = serde_json::json!({
          "EIP712Domain": [
          ],
          "Message":[
          {"name":"payload", "type":"bytes"},
          {"name":"payloadType", "type":"string"}
          ]
        });
        let types: Types = serde_json::from_value(json).unwrap();
        assert_eq!(2, types.len());
        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        message.insert(
            "payload".to_string(),
            serde_json::Value::from(format!("{payload_session_info}")),
        );
        message.insert("payloadType".to_string(), serde_json::Value::from("0"));
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
        let kp = get_a_static_keypair();
        let signer = Db3MultiSchemeSigner::new(kp);
        let sign_account = AccountId::new(signer.get_address().unwrap());
        let signature_raw = signer.sign_typed_data(&typed_data)?;
        let typed_data_buf = serde_json::to_vec(&typed_data).unwrap();
        match verify_query_session(
            typed_data_buf.as_ref(),
            PayloadType::TypedDataPayload as i32,
            signature_raw.as_ref(),
        ) {
            Ok((session_info, account_id)) => {
                println!(
                    "{:?} \n {:?}",
                    serde_json::to_string(&sign_account.addr).unwrap(),
                    serde_json::to_string(&account_id.addr).unwrap()
                );
                assert!(sign_account.addr == account_id.addr);
                assert_eq!(query_session_info, session_info)
            }
            Err(e) => {
                println!("{e}");
                assert!(false)
            }
        }
        Ok(())
    }

    #[test]
    fn test_verify_protobuf_happy_path() -> Result<()> {
        let meta = BroadcastMeta {
            //TODO get from network
            nonce: 10,
            //TODO use config
            chain_id: ChainId::DevNet.into(),
            //TODO use config
            chain_role: ChainRole::StorageShardChain.into(),
        };

        // the client query session
        let query_session_info = QuerySessionInfo {
            id: 1,
            start_time: Utc::now().timestamp(),
            query_count: 10,
            meta: Some(meta),
        };

        let mut buf = BytesMut::with_capacity(1024 * 8);
        query_session_info.encode(&mut buf).unwrap();
        let payload_session_info = buf.freeze();
        let kp = get_a_static_keypair();
        let signer = Db3MultiSchemeSigner::new(kp);
        let sign_account = AccountId::new(signer.get_address().unwrap());
        let signature_raw = signer.sign(payload_session_info.as_ref())?;
        match verify_query_session(
            payload_session_info.as_ref(),
            PayloadType::QuerySessionPayload as i32,
            signature_raw.as_ref(),
        ) {
            Ok((session_info, account_id)) => {
                assert!(sign_account.addr == account_id.addr);
                assert_eq!(query_session_info, session_info)
            }
            Err(_) => {
                assert!(false)
            }
        }
        Ok(())
    }
}
