//
// mutation_sdk.rs
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

use ethers::core::types::{
    transaction::eip712::{EIP712Domain, TypedData, Types},
    Bytes,
};

use bytes::BytesMut;
use db3_crypto::{
    db3_signer::Db3MultiSchemeSigner,
    id::{DbId, TxId, TX_ID_LENGTH},
};

use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_proto::{
    DatabaseMutation, MintCreditsMutation, PayloadType, WriteRequest,
};

use db3_proto::db3_node_proto::{storage_node_client::StorageNodeClient, BroadcastRequest};
use prost::Message;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct MutationSDK {
    signer: Db3MultiSchemeSigner,
    client: Arc<StorageNodeClient<tonic::transport::Channel>>,
    types: Types,
    use_typed_format: bool,
}

impl MutationSDK {
    pub fn new(
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        signer: Db3MultiSchemeSigner,
        use_typed_format: bool,
    ) -> Self {
        let json = serde_json::json!({
          "EIP712Domain": [
          ],
          "Message":[
          {"name":"payload", "type":"bytes"},
          {"name":"payloadType", "type":"string"}
          ]
        });
        let types: Types = serde_json::from_value(json).unwrap();
        Self {
            client,
            signer,
            types,
            use_typed_format,
        }
    }

    pub async fn submit_mint_credit_mutation(
        &self,
        mutation: &MintCreditsMutation,
    ) -> Result<TxId> {
        let _nonce: u64 = match &mutation.meta {
            Some(m) => Ok(m.nonce),
            None => Err(DB3Error::SubmitMutationError(
                "meta in mutation is none".to_string(),
            )),
        }?;
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        mutation
            .encode(&mut mbuf)
            .map_err(|e| DB3Error::SubmitMutationError(format!("{e}")))?;
        let mbuf = mbuf.freeze();
        let signature = self.signer.sign(mbuf.as_ref())?;
        let request = WriteRequest {
            signature: signature.as_ref().to_vec().to_owned(),
            payload: mbuf.as_ref().to_vec().to_owned(),
            payload_type: PayloadType::MintCreditsPayload.into(),
        };
        let mut buf = BytesMut::with_capacity(1024 * 4);
        request
            .encode(&mut buf)
            .map_err(|e| DB3Error::SubmitMutationError(format!("{e}")))?;
        let buf = buf.freeze();
        let r = BroadcastRequest {
            body: buf.as_ref().to_vec(),
        };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let response = client
            .broadcast(request)
            .await
            .map_err(|e| DB3Error::SubmitMutationError(format!("{e}")))?
            .into_inner();
        let hash: [u8; TX_ID_LENGTH] = response
            .hash
            .try_into()
            .map_err(|_| DB3Error::InvalidAddress)?;
        let tx_id = TxId::from(hash);
        Ok(tx_id)
    }

    fn wrap_typed_database_mutation(
        &self,
        database_mutation: &DatabaseMutation,
    ) -> Result<WriteRequest> {
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        database_mutation
            .encode(&mut mbuf)
            .map_err(|e| DB3Error::SubmitMutationError(format!("{e}")))?;
        let mbuf = Bytes(mbuf.freeze());
        let mut message: BTreeMap<String, serde_json::Value> = BTreeMap::new();
        message.insert(
            "payload".to_string(),
            serde_json::Value::from(format!("{mbuf}")),
        );
        message.insert("payloadType".to_string(), serde_json::Value::from("1"));
        let typed_data = TypedData {
            domain: EIP712Domain {
                name: None,
                version: None,
                chain_id: None,
                verifying_contract: None,
                salt: None,
            },
            types: self.types.clone(),
            primary_type: "Message".to_string(),
            message,
        };
        let signature = self.signer.sign_typed_data(&typed_data)?;
        let buf = serde_json::to_vec(&typed_data)
            .map_err(|e| DB3Error::SubmitMutationError(format!("{e}")))?;
        let request = WriteRequest {
            signature,
            payload: buf,
            payload_type: PayloadType::TypedDataPayload.into(),
        };
        Ok(request)
    }

    fn wrap_proto_database_mutation(
        &self,
        database_mutation: &DatabaseMutation,
    ) -> Result<WriteRequest> {
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        database_mutation
            .encode(&mut mbuf)
            .map_err(|e| DB3Error::SubmitMutationError(format!("{e}")))?;
        let mbuf = mbuf.freeze();
        let signature = self.signer.sign(mbuf.as_ref())?;
        let request = WriteRequest {
            signature: signature.as_ref().to_vec().to_owned(),
            payload: mbuf.as_ref().to_vec().to_owned(),
            payload_type: PayloadType::DatabasePayload.into(),
        };
        Ok(request)
    }

    pub async fn submit_database_mutation(
        &self,
        database_mutation: &DatabaseMutation,
    ) -> Result<(DbId, TxId)> {
        let nonce: u64 = match &database_mutation.meta {
            Some(m) => Ok(m.nonce),
            None => Err(DB3Error::SubmitMutationError(
                "meta in mutation is none".to_string(),
            )),
        }?;
        let request = match self.use_typed_format {
            true => {
                let r = self.wrap_typed_database_mutation(database_mutation)?;
                r
            }
            false => {
                let r = self.wrap_proto_database_mutation(database_mutation)?;
                r
            }
        };
        //TODO generate the address from local currently
        //
        let mut buf = BytesMut::with_capacity(1024 * 4);
        request
            .encode(&mut buf)
            .map_err(|e| DB3Error::SubmitMutationError(format!("{e}")))?;
        let buf = buf.freeze();
        let r = BroadcastRequest {
            body: buf.as_ref().to_vec(),
        };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let response = client
            .broadcast(request)
            .await
            .map_err(|e| DB3Error::SubmitMutationError(format!("{e}")))?
            .into_inner();
        let hash: [u8; TX_ID_LENGTH] = response
            .hash
            .try_into()
            .map_err(|_| DB3Error::InvalidAddress)?;
        let tx_id = TxId::from(hash);
        let sender = self.signer.get_address()?;
        let db_id = DbId::try_from((&sender, nonce))?;
        Ok((db_id, tx_id))
    }
}

#[cfg(test)]
mod tests {
    use super::MutationSDK;
    use crate::mutation_sdk::StorageNodeClient;
    use crate::sdk_test;
    use crate::store_sdk::StoreSDK;
    use std::sync::Arc;
    use std::{thread, time};
    use tonic::transport::Endpoint;

    async fn run_mint_credits_mutation_flow(
        use_typed_format: bool,
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        counter: i64,
    ) {
        let (to_address, _signer) = sdk_test::gen_secp256k1_signer(counter + 1);
        let (sender_address, signer) = sdk_test::gen_secp256k1_signer(counter + 2);
        let sdk = MutationSDK::new(client.clone(), signer, use_typed_format);
        let dm = sdk_test::create_a_mint_mutation(&sender_address, &to_address);
        let result = sdk.submit_mint_credit_mutation(&dm).await;
        assert!(result.is_ok());
        let millis = time::Duration::from_millis(2000);
        thread::sleep(millis);
        let (_, signer) = sdk_test::gen_secp256k1_signer(counter + 1);
        let store_sdk = StoreSDK::new(client, signer, use_typed_format);
        let account = store_sdk.get_account(&to_address).await.unwrap();
        assert_eq!(account.credits, 9 * 1000_000_000);
    }

    async fn run_database_mutation_flow(
        use_typed_format: bool,
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        counter: i64,
    ) {
        let (_, signer) = sdk_test::gen_secp256k1_signer(counter);
        let sdk = MutationSDK::new(client.clone(), signer, use_typed_format);
        let dm = sdk_test::create_a_database_mutation();
        let result = sdk.submit_database_mutation(&dm).await;
        assert!(result.is_ok());
        let (db_id, _) = result.unwrap();
        let millis = time::Duration::from_millis(2000);
        thread::sleep(millis);
        let (_, signer) = sdk_test::gen_secp256k1_signer(counter);
        let mut store_sdk = StoreSDK::new(client, signer, use_typed_format);
        let database_ret = store_sdk.get_database(db_id.to_hex().as_str()).await;
        assert!(database_ret.is_ok());
        assert!(database_ret.unwrap().is_some());
        let result = store_sdk.close_session().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn smoke_test_run_mint_credits_mutation_flow() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        run_mint_credits_mutation_flow(false, client.clone(), 10).await;
        run_mint_credits_mutation_flow(true, client.clone(), 100).await;
    }

    #[tokio::test]
    async fn it_database_mutation_smoke_test() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        run_database_mutation_flow(false, client.clone(), 20).await;
        run_database_mutation_flow(true, client.clone(), 30).await;
    }
}
