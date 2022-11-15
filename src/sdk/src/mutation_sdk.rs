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
use db3_crypto::signer::Db3Signer;
use db3_error::{DB3Error, Result};
use db3_proto::db3_mutation_proto::{Mutation, WriteRequest};
use db3_proto::db3_node_proto::{storage_node_client::StorageNodeClient, BroadcastRequest};
use prost::Message;
use std::sync::Arc;

pub struct MutationSDK {
    signer: Db3Signer,
    client: Arc<StorageNodeClient<tonic::transport::Channel>>,
}

impl MutationSDK {
    pub fn new(
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        signer: Db3Signer,
    ) -> Self {
        Self { client, signer }
    }

    pub async fn submit_mutation(&self, mutation: &Mutation) -> Result<Vec<u8>> {
        //TODO update gas and nonce
        let mut mbuf = BytesMut::with_capacity(1024 * 4);
        mutation
            .encode(&mut mbuf)
            .map_err(|e| DB3Error::SubmitMutationError(format!("{}", e)))?;
        let mbuf = mbuf.freeze();
        let signature = self.signer.sign(mbuf.as_ref())?;
        let request = WriteRequest {
            signature,
            mutation: mbuf.as_ref().to_vec().to_owned(),
        };
        let mut buf = BytesMut::with_capacity(1024 * 4);
        request
            .encode(&mut buf)
            .map_err(|e| DB3Error::SubmitMutationError(format!("{}", e)))?;
        let buf = buf.freeze();
        let r = BroadcastRequest {
            body: buf.as_ref().to_vec(),
        };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let response = client
            .broadcast(request)
            .await
            .map_err(|e| DB3Error::SubmitMutationError(format!("{}", e)))?
            .into_inner();
        Ok(response.hash)
    }
}

#[cfg(test)]
mod tests {
    use super::Db3Signer;
    use super::Mutation;
    use super::MutationSDK;
    use crate::mutation_sdk::StorageNodeClient;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{KvPair, MutationAction};
    use fastcrypto::secp256k1::Secp256k1KeyPair;
    use fastcrypto::traits::KeyPair;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::Arc;
    use std::{thread, time};
    use tonic::transport::Endpoint;
    #[tokio::test]
    async fn test_submit_mutation() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let mut rng = StdRng::from_seed([0; 32]);
        let kp = Secp256k1KeyPair::generate(&mut rng);
        let signer = Db3Signer::new(kp);
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
                nonce: 11000,
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
