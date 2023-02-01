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

use bytes::BytesMut;
use db3_crypto::{
    db3_signer::Db3MultiSchemeSigner,
    id::{DbId, TxId, TX_ID_LENGTH},
};
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_proto::{DatabaseMutation, Mutation, PayloadType, WriteRequest};
use db3_proto::db3_node_proto::{storage_node_client::StorageNodeClient, BroadcastRequest};
use prost::Message;
use std::sync::Arc;

pub struct MutationSDK {
    signer: Db3MultiSchemeSigner,
    client: Arc<StorageNodeClient<tonic::transport::Channel>>,
}

impl MutationSDK {
    pub fn new(
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        signer: Db3MultiSchemeSigner,
    ) -> Self {
        Self { client, signer }
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
        //
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

    pub async fn submit_mutation(&self, mutation: &Mutation) -> Result<TxId> {
        //TODO update gas and nonce
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        mutation
            .encode(&mut mbuf)
            .map_err(|e| DB3Error::SubmitMutationError(format!("{e}")))?;
        let mbuf = mbuf.freeze();
        let signature = self.signer.sign(mbuf.as_ref())?;
        let request = WriteRequest {
            signature: signature.as_ref().to_vec().to_owned(),
            payload: mbuf.as_ref().to_vec().to_owned(),
            payload_type: PayloadType::MutationPayload.into(),
        };

        //TODO add the capacity to mutation sdk configuration
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
}

#[cfg(test)]
mod tests {
    use super::Mutation;
    use super::MutationSDK;
    use crate::mutation_sdk::StorageNodeClient;
    use crate::sdk_test;
    use crate::store_sdk::StoreSDK;
    use db3_base::get_a_random_nonce;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{KvPair, MutationAction};
    use rand::Rng;
    use std::sync::Arc;
    use std::{thread, time};
    use tonic::transport::Endpoint;

    #[tokio::test]
    async fn test_submit_duplicated_key_mutation() {
        let nonce = get_a_random_nonce();
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let (_, signer) = sdk_test::gen_secp256k1_signer();
        let ns = "my_twitter";
        {
            let sdk = MutationSDK::new(client.clone(), signer);
            let kv = KvPair {
                key: format!("kk{}", 1).as_bytes().to_vec(),
                value: format!("dkalue{}", 1).as_bytes().to_vec(),
                action: MutationAction::InsertKv.into(),
            };
            let mutation = Mutation {
                ns: ns.as_bytes().to_vec(),
                kv_pairs: vec![kv],
                nonce,
                chain_id: ChainId::MainNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
                gas_price: None,
                gas: 10,
            };
            // submit ok
            let result = sdk.submit_mutation(&mutation).await;
            assert!(result.is_ok());
            let kv = KvPair {
                key: format!("dkkkk{}", 1).as_bytes().to_vec(),
                value: format!("dkalue{}", 1).as_bytes().to_vec(),
                action: MutationAction::InsertKv.into(),
            };
            let mutation = Mutation {
                ns: ns.as_bytes().to_vec(),
                kv_pairs: vec![kv.clone(), kv],
                nonce,
                chain_id: ChainId::MainNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
                gas_price: None,
                gas: 10,
            };
            let result = sdk.submit_mutation(&mutation).await;
            assert!(result.is_ok());
            // submit ok
        }
        let millis = time::Duration::from_millis(2000);
        thread::sleep(millis);
        let (_, signer) = sdk_test::gen_secp256k1_signer();
        let mut store_sdk = StoreSDK::new(client, signer);
        let sess_token = store_sdk.open_session().await.unwrap().session_token;
        let values = store_sdk
            .batch_get(
                ns.as_bytes(),
                vec!["dkkk1".as_bytes().to_vec()],
                &sess_token,
            )
            .await
            .unwrap();
        assert!(!values.is_none());
        assert_eq!(values.unwrap().values.len(), 0);
        store_sdk.close_session(&sess_token).await.unwrap();
    }

    #[tokio::test]
    async fn test_submit_mutation() {
        let mut rng = rand::thread_rng();
        let nonce = rng.gen_range(0..11100);
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let (_, signer) = sdk_test::gen_secp256k1_signer();
        let sdk = MutationSDK::new(client, signer);
        let mut count = 1;
        loop {
            let kv = KvPair {
                key: format!("kkkkk{}", count).as_bytes().to_vec(),
                value: format!("vkalue{}", count).as_bytes().to_vec(),
                action: MutationAction::InsertKv.into(),
            };
            let mutation = Mutation {
                ns: "my_twitter".as_bytes().to_vec(),
                kv_pairs: vec![kv],
                nonce,
                chain_id: ChainId::MainNet.into(),
                chain_role: ChainRole::StorageShardChain.into(),
                gas_price: None,
                gas: 10,
            };
            let result = sdk.submit_mutation(&mutation).await;
            assert!(result.is_ok());
            let ten_millis = time::Duration::from_millis(1000);
            thread::sleep(ten_millis);
            count = count + 1;
            if count > 10 {
                break;
            }
        }
    }
}
