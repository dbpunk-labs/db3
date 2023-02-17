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
use db3_proto::db3_mutation_proto::{DatabaseMutation, PayloadType, WriteRequest};
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
}

#[cfg(test)]
mod tests {
    use super::MutationSDK;
    use crate::mutation_sdk::StorageNodeClient;
    use crate::sdk_test;
    use crate::store_sdk::StoreSDK;
    use db3_base::get_a_random_nonce;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use rand::Rng;
    use std::sync::Arc;
    use std::{thread, time};
    use tonic::transport::Endpoint;

    #[tokio::test]
    async fn it_database_mutation_smoke_test() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let (_, signer) = sdk_test::gen_secp256k1_signer();
        let sdk = MutationSDK::new(client.clone(), signer);
        let dm = sdk_test::create_a_database_mutation();
        let result = sdk.submit_database_mutation(&dm).await;
        assert!(result.is_ok());
        let (db_id, _) = result.unwrap();
        let millis = time::Duration::from_millis(2000);
        thread::sleep(millis);
        let (_, signer) = sdk_test::gen_secp256k1_signer();
        let mut store_sdk = StoreSDK::new(client, signer);
        let database_ret = store_sdk.get_database(db_id.to_hex().as_str()).await;
        assert!(database_ret.is_ok());
        assert!(database_ret.unwrap().is_some());
        let result = store_sdk.close_session().await;
        assert!(result.is_ok());
    }
}
