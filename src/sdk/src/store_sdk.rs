//
// bill_sdk.rs
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
use db3_crypto::signer::Db3Signer;
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_bill_proto::Bill;
use db3_proto::db3_node_proto::{
    storage_node_client::StorageNodeClient, BatchGetKey, BatchGetValue, CloseSessionPayload,
    CloseSessionRequest, CloseSessionResponse, GetAccountRequest, GetKeyRequest,
    GetSessionInfoRequest, OpenSessionRequest, OpenSessionResponse, QueryBillKey, QueryBillRequest,
    QuerySessionInfo, SessionIdentifier,
};
use db3_session::session_manager::SessionPool;
use ethereum_types::Address as AccountAddress;
use prost::Message;
use std::sync::Arc;
use tonic::Status;

pub struct StoreSDK {
    client: Arc<StorageNodeClient<tonic::transport::Channel>>,
    signer: Db3Signer,
    session_pool: SessionPool,
}

impl StoreSDK {
    pub fn new(
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        signer: Db3Signer,
    ) -> Self {
        Self {
            client,
            signer,
            session_pool: SessionPool::new(),
        }
    }

    pub async fn open_session(&mut self) -> std::result::Result<OpenSessionResponse, Status> {
        let buf = "Header".as_bytes();
        let signature = self
            .signer
            .sign(buf.as_ref())
            .map_err(|e| Status::internal(format!("{:?}", e)))?;
        let r = OpenSessionRequest {
            header: buf.to_vec(),
            signature,
        };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let response = client.open_query_session(request).await?.into_inner();
        let result = response.clone();
        match self
            .session_pool
            .insert_session_with_token(&result.query_session_info.unwrap(), &result.session_token)
        {
            Ok(_) => Ok(response.clone()),
            Err(e) => Err(Status::internal(format!("Fail to create session {}", e))),
        }
    }
    /// close session
    /// 1. verify Account
    /// 2. request close_query_session
    /// 3. return node's CloseSessionResponse(query session info and signature) and client's CloseSessionResponse (query session info and signature)
    pub async fn close_session(
        &mut self,
        token: &String,
    ) -> std::result::Result<(CloseSessionResponse, CloseSessionResponse), Status> {
        match self.session_pool.get_session(token) {
            Some(sess) => {
                let query_session_info = sess.get_session_info();
                let payload = CloseSessionPayload {
                    session_info: Some(query_session_info.clone()),
                    session_token: token.clone(),
                };
                let mut buf = BytesMut::with_capacity(1024 * 8);
                payload
                    .encode(&mut buf)
                    .map_err(|e| Status::internal(format!("{}", e)))?;
                let buf = buf.freeze();
                let signature = self
                    .signer
                    .sign(buf.as_ref())
                    .map_err(|e| Status::internal(format!("{:?}", e)))?;
                let r = CloseSessionRequest {
                    payload: buf.as_ref().to_vec(),
                    signature: signature.clone(),
                };
                let request = tonic::Request::new(r);
                let mut client = self.client.as_ref().clone();
                match client.close_query_session(request).await {
                    Ok(response) => match self.session_pool.remove_session(token) {
                        Ok(_) => Ok((
                            response.into_inner(),
                            CloseSessionResponse {
                                query_session_info: Some(query_session_info),
                            },
                        )),
                        Err(e) => Err(Status::internal(format!("{}", e))),
                    },
                    Err(e) => Err(e),
                }
            }
            None => Err(Status::internal(format!("Session {} not exist", token))),
        }
    }

    pub async fn get_bills_by_block(
        &mut self,
        height: u64,
        start: u64,
        end: u64,
        token: &String,
    ) -> std::result::Result<Vec<Bill>, Status> {
        match self.session_pool.get_session_mut(token) {
            Some(session) => {
                if session.check_session_running() {
                    let mut client = self.client.as_ref().clone();
                    let query_bill_key = Some(QueryBillKey {
                        height,
                        start_id: start,
                        end_id: end,
                        session_token: token.clone(),
                    });
                    let q_req = QueryBillRequest { query_bill_key };
                    let request = tonic::Request::new(q_req);
                    let response = client.query_bill(request).await?.into_inner();
                    session.increase_query(1);
                    Ok(response.bills)
                } else {
                    Err(Status::permission_denied(
                        "Fail to query bill in this session. Please restart query session",
                    ))
                }
            }
            None => Err(Status::not_found(format!(
                "Fail to query, session with token {} not found",
                token
            ))),
        }
    }

    pub async fn get_account(&self, addr: &AccountAddress) -> std::result::Result<Account, Status> {
        let r = GetAccountRequest {
            addr: format!("{:?}", addr),
        };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let account = client.get_account(request).await?.into_inner();
        Ok(account)
    }

    pub async fn get_session_info(
        &self,
        session_token: &String,
    ) -> std::result::Result<QuerySessionInfo, Status> {
        let session_identifier = Some(SessionIdentifier {
            session_token: session_token.clone(),
        });
        let r = GetSessionInfoRequest { session_identifier };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();

        let response = client.get_session_info(request).await?.into_inner();
        Ok(response.session_info.unwrap())
    }

    pub async fn batch_get(
        &mut self,
        ns: &[u8],
        keys: Vec<Vec<u8>>,
        token: &String,
    ) -> std::result::Result<Option<BatchGetValue>, Status> {
        match self.session_pool.get_session_mut(token) {
            Some(session) => {
                if session.check_session_running() {
                    let batch_get = Some(BatchGetKey {
                        ns: ns.to_vec(),
                        keys,
                        session_token: token.clone(),
                    });
                    let r = GetKeyRequest { batch_get };
                    let request = tonic::Request::new(r);

                    let mut client = self.client.as_ref().clone();
                    let response = client.get_key(request).await?.into_inner();
                    // TODO(cj): batch keys query should be count as a query or multi queries?
                    session.increase_query(1);
                    Ok(response.batch_get_values)
                } else {
                    Err(Status::permission_denied(
                        "Fail to query in this session. Please restart query session",
                    ))
                }
            }
            None => Err(Status::not_found(format!(
                "Fail to query, session with token {} not found",
                token
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Db3Signer;
    use super::StoreSDK;
    use crate::mutation_sdk::MutationSDK;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::KvPair;
    use db3_proto::db3_mutation_proto::{Mutation, MutationAction};
    use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
    use fastcrypto::secp256k1::Secp256k1KeyPair;
    use fastcrypto::traits::KeyPair;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::Arc;
    use std::time;
    use tonic::transport::Endpoint;
    #[tokio::test]
    async fn it_get_bills() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mclient = client.clone();
        {
            let mut rng = StdRng::from_seed([0; 32]);
            let kp = Secp256k1KeyPair::generate(&mut rng);
            let signer = Db3Signer::new(kp);
            let msdk = MutationSDK::new(mclient, signer);
            let kv = KvPair {
                key: format!("kkkkk_tt{}", 1).as_bytes().to_vec(),
                value: format!("vkalue_tt{}", 1).as_bytes().to_vec(),
                action: MutationAction::InsertKv.into(),
            };
            let mutation = Mutation {
                ns: "my_twitter".as_bytes().to_vec(),
                kv_pairs: vec![kv],
                nonce: 11000,
                chain_id: ChainId::MainNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
                gas_price: None,
                gas: 10,
            };
            let result = msdk.submit_mutation(&mutation).await;
            assert!(result.is_ok());
            let ten_millis = time::Duration::from_millis(1000);
            std::thread::sleep(ten_millis);
        }
        let mut rng = StdRng::from_seed([0; 32]);
        let kp = Secp256k1KeyPair::generate(&mut rng);
        let signer = Db3Signer::new(kp);
        let mut sdk = StoreSDK::new(client, signer);
        let res = sdk.open_session().await;
        assert!(res.is_ok());
        let session_info = res.unwrap();
        assert_eq!(session_info.session_token.len(), 36);
        let result = sdk
            .get_bills_by_block(1, 0, 10, &session_info.session_token)
            .await;
        if let Err(ref e) = result {
            println!("{}", e);
            assert!(false);
        }
        assert!(result.is_ok());
    }
    #[tokio::test]
    async fn close_session_happy_path() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mclient = client.clone();
        let key_vec = format!("kkkkk_tt{}", 10).as_bytes().to_vec();
        let value_vec = format!("vkalue_tt{}", 10).as_bytes().to_vec();
        let ns_vec = "my_twitter".as_bytes().to_vec();
        {
            let mut rng = StdRng::from_seed([0; 32]);
            let kp = Secp256k1KeyPair::generate(&mut rng);
            let signer = Db3Signer::new(kp);
            let msdk = MutationSDK::new(mclient, signer);
            let kv = KvPair {
                key: key_vec.clone(),
                value: value_vec.clone(),
                action: MutationAction::InsertKv.into(),
            };
            let mutation = Mutation {
                ns: ns_vec.clone(),
                kv_pairs: vec![kv],
                nonce: 11000,
                chain_id: ChainId::MainNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
                gas_price: None,
                gas: 10,
            };
            let result = msdk.submit_mutation(&mutation).await;
            assert!(result.is_ok(), "{}", result.err().unwrap());
            let ten_millis = time::Duration::from_millis(1000);
            std::thread::sleep(ten_millis);
        }
        let mut rng = StdRng::from_seed([0; 32]);
        let kp = Secp256k1KeyPair::generate(&mut rng);
        let signer = Db3Signer::new(kp);
        let mut sdk = StoreSDK::new(client, signer);
        let res = sdk.open_session().await;
        assert!(res.is_ok());
        let session_info = res.unwrap();
        assert_eq!(session_info.session_token.len(), 36);
        if let Ok(Some(values)) = sdk
            .batch_get(&ns_vec, vec![key_vec.clone()], &session_info.session_token)
            .await
        {
            assert_eq!(values.values.len(), 1);
            assert_eq!(values.values[0].key.to_vec(), key_vec);
            assert_eq!(values.values[0].value.to_vec(), value_vec);
        } else {
            assert!(false);
        }

        let res = sdk.close_session(&session_info.session_token).await;
        assert!(res.is_ok());
    }
    #[tokio::test]
    async fn close_session_wrong_path() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mclient = client.clone();
        let key_vec = format!("kkkkk_tt{}", 20).as_bytes().to_vec();
        let value_vec = format!("vkalue_tt{}", 20).as_bytes().to_vec();
        let ns_vec = "my_twitter".as_bytes().to_vec();
        {
            let mut rng = StdRng::from_seed([0; 32]);
            let kp = Secp256k1KeyPair::generate(&mut rng);
            let signer = Db3Signer::new(kp);
            let msdk = MutationSDK::new(mclient, signer);
            let kv = KvPair {
                key: key_vec.clone(),
                value: value_vec.clone(),
                action: MutationAction::InsertKv.into(),
            };
            let mutation = Mutation {
                ns: ns_vec.clone(),
                kv_pairs: vec![kv],
                nonce: 11000,
                chain_id: ChainId::MainNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
                gas_price: None,
                gas: 10,
            };
            let result = msdk.submit_mutation(&mutation).await;
            assert!(result.is_ok(), "{}", result.err().unwrap());
            let ten_millis = time::Duration::from_millis(1000);
            std::thread::sleep(ten_millis);
        }
        let mut rng = StdRng::from_seed([0; 32]);
        let kp = Secp256k1KeyPair::generate(&mut rng);
        let signer = Db3Signer::new(kp);
        let mut sdk = StoreSDK::new(client, signer);
        let res = sdk.open_session().await;
        assert!(res.is_ok());
        let session_info = res.unwrap();
        assert_eq!(session_info.session_token.len(), 36);
        if let Ok(Some(values)) = sdk
            .batch_get(&ns_vec, vec![key_vec.clone()], &session_info.session_token)
            .await
        {
            assert_eq!(values.values.len(), 1);
            assert_eq!(values.values[0].key.to_vec(), key_vec);
            assert_eq!(values.values[0].value.to_vec(), value_vec);
        } else {
            assert!(false);
        }

        sdk.session_pool
            .get_session_mut(&session_info.session_token)
            .unwrap()
            .increase_query(100);
        let res = sdk.close_session(&session_info.session_token).await;
        assert!(res.is_err());
        assert_eq!(
            res.err().unwrap().message(),
            "query session verify fail. expect query count 1 but 101"
        );
    }
}
