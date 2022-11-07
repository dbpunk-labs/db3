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
    storage_node_client::StorageNodeClient, BatchGetKey, BatchGetValue, GetAccountRequest,
    GetKeyRequest, QueryBillRequest,
};
use ethereum_types::Address as AccountAddress;
use prost::Message;
use std::sync::Arc;
use tonic::Status;

pub struct StoreSDK {
    client: Arc<StorageNodeClient<tonic::transport::Channel>>,
    signer: Db3Signer,
}

impl StoreSDK {
    pub fn new(
        client: Arc<StorageNodeClient<tonic::transport::Channel>>,
        signer: Db3Signer,
    ) -> Self {
        Self { client, signer }
    }

    pub async fn get_bills_by_block(
        &self,
        height: u64,
        start: u64,
        end: u64,
    ) -> std::result::Result<Vec<Bill>, Status> {
        let mut client = self.client.as_ref().clone();
        let q_req = QueryBillRequest {
            height,
            start_id: start,
            end_id: end,
        };
        let request = tonic::Request::new(q_req);
        let response = client.query_bill(request).await?.into_inner();
        Ok(response.bills)
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

    pub async fn batch_get(
        &self,
        ns: &[u8],
        keys: Vec<Vec<u8>>,
    ) -> std::result::Result<Option<BatchGetValue>, Status> {
        let batch_keys = BatchGetKey {
            ns: ns.to_vec(),
            keys,
            session: 1,
        };
        let mut buf = BytesMut::with_capacity(1024 * 8);
        batch_keys
            .encode(&mut buf)
            .map_err(|e| Status::internal(format!("{}", e)))?;
        let buf = buf.freeze();
        let signature = self
            .signer
            .sign(buf.as_ref())
            .map_err(|e| Status::internal(format!("{}", e)))?;
        let r = GetKeyRequest {
            batch_get: buf.as_ref().to_vec(),
            signature,
        };
        let request = tonic::Request::new(r);
        let mut client = self.client.as_ref().clone();
        let response = client.get_key(request).await?.into_inner();
        Ok(response.batch_get_values)
    }
}

#[cfg(test)]
mod tests {
    use super::Db3Signer;
    use super::StoreSDK;
    use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
    use fastcrypto::secp256k1::Secp256k1KeyPair;
    use fastcrypto::traits::KeyPair;
    use rand::rngs::StdRng;
    use rand::SeedableRng;
    use std::sync::Arc;
    use tonic::transport::Endpoint;
    #[ignore]
    #[tokio::test]
    async fn it_get_bills() {
        let mut rng = StdRng::from_seed([0; 32]);
        let kp = Secp256k1KeyPair::generate(&mut rng);
        let signer = Db3Signer::new(kp);
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let sdk = StoreSDK::new(client, signer);
        let result = sdk.get_bills_by_block(1, 0, 10).await;
        if let Err(ref e) = result {
            println!("{}", e);
        }
        assert!(result.is_ok());
        if let Ok(bills) = result {
            assert_eq!(0, bills.len());
        }
    }
}
