//
//
// compute_node_sdk.rs
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
//
use crate::proto::db3_base_proto::FlightData;
use crate::proto::db3_compute_proto::compute_node_client::ComputeNodeClient;
use crate::proto::db3_compute_proto::QueryRequest;

use std::sync::Arc;
use tonic::transport::Endpoint;
use tonic::{Response, Status};

pub struct ComputeNodeSDK {
    endpoint: String,
    // clone on use
    client: Arc<ComputeNodeClient<tonic::transport::Channel>>,
}

impl Clone for ComputeNodeSDK {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.to_string(),
            client: self.client.clone(),
        }
    }
}

impl ComputeNodeSDK {
    pub async fn connect(endpoint: &str) -> std::result::Result<Self, tonic::transport::Error> {
        // create a new client connection
        let rpc_endpoint = Endpoint::new(endpoint.to_string())?;
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(ComputeNodeClient::new(channel));
        Ok(ComputeNodeSDK {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    pub async fn query(
        &self,
        sql: &str,
        db: &str,
        cnn_id: u32,
    ) -> std::result::Result<Response<tonic::codec::Streaming<FlightData>>, Status> {
        let mut client = self.client.as_ref().clone();
        let query_req = QueryRequest {
            default_db: db.to_string(),
            sql: sql.to_string(),
            cnn_id,
        };
        client.query(query_req).await
    }
}
