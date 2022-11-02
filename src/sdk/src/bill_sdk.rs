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
use db3_error::{DB3Error, Result};
use db3_proto::db3_bill_proto::{Bill, BillQueryRequest};
use db3_proto::db3_node_proto::{
    storage_node_client::StorageNodeClient, QueryBillRequest, QueryBillResponse,
};
use std::sync::Arc;
use tonic::Status;

#[derive(Debug, Clone)]
pub struct BillSDK {
    client: Arc<StorageNodeClient<tonic::transport::Channel>>,
}

impl BillSDK {
    pub fn new(client: Arc<StorageNodeClient<tonic::transport::Channel>>) -> Self {
        Self { client }
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
}

#[cfg(test)]
mod tests {
    use super::BillSDK;
    use db3_proto::db3_node_proto::storage_node_client::StorageNodeClient;
    use std::sync::Arc;
    use tonic::transport::Endpoint;
    #[tokio::test]
    async fn it_get_bills() {
        let ep = "http://127.0.0.1:26659";
        let rpc_endpoint = Endpoint::new(ep.to_string()).unwrap();
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(StorageNodeClient::new(channel));
        let sdk = BillSDK::new(client);
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
