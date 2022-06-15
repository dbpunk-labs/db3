//
//
// memory_node_sdk.rs
// Copyright (C) 2022 rtstore.io Author imotai <codego.me@gmail.com>
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

use crate::codec::row_codec::{encode, RowRecordBatch};
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{
    FlightData, RtStoreTableDesc, StorageBackendConfig, StorageRegion,
};
use crate::proto::rtstore_memory_proto::memory_node_client::MemoryNodeClient;
use crate::proto::rtstore_memory_proto::{
    AppendRecordsRequest, AppendRecordsResponse, AssignPartitionRequest, AssignPartitionResponse,
    FetchPartitionRequest,
};

use std::sync::Arc;
use tonic::transport::Endpoint;
use tonic::{Request, Response, Status};

pub struct MemoryNodeSDK {
    endpoint: String,
    // clone on use
    client: Arc<MemoryNodeClient<tonic::transport::Channel>>,
}

impl Clone for MemoryNodeSDK {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.to_string(),
            client: self.client.clone(),
        }
    }
}

impl MemoryNodeSDK {
    pub async fn connect(endpoint: &str) -> std::result::Result<Self, tonic::transport::Error> {
        // create a new client connection
        let rpc_endpoint = Endpoint::new(endpoint.to_string())?;
        let channel = rpc_endpoint.connect_lazy();
        let client = Arc::new(MemoryNodeClient::new(channel));
        Ok(MemoryNodeSDK {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    pub async fn assign_partition(
        &self,
        partition_ids: &[i32],
        table_desc: &RtStoreTableDesc,
        storage_config: &StorageBackendConfig,
    ) -> std::result::Result<(), Status> {
        let mut client = self.client.as_ref().clone();
        let assign_req = AssignPartitionRequest {
            partition_ids: partition_ids.to_vec(),
            table_desc: Some(table_desc.clone()),
            config: Some(storage_config.clone()),
        };
        let request = tonic::Request::new(assign_req);
        client.assign_partition(request).await?;
        Ok(())
    }

    pub async fn get_head_batch_of_partition(
        &self,
        db: &str,
        table: &str,
        partition_id: i32,
    ) -> std::result::Result<Response<tonic::codec::Streaming<FlightData>>, Status> {
        let mut client = self.client.as_ref().clone();
        let fetch_req = FetchPartitionRequest {
            table_id: table.to_string(),
            db: db.to_string(),
            partition_id,
        };
        client.fetch_partition(fetch_req).await
    }

    pub async fn append_records(
        &self,
        db: &str,
        table_id: &str,
        partition_id: i32,
        record: &RowRecordBatch,
    ) -> std::result::Result<(), Status> {
        let data = encode(record)?;
        let mut client = self.client.as_ref().clone();
        let append_records_req = AppendRecordsRequest {
            table_id: table_id.to_string(),
            partition_id,
            records: data,
            db: db.to_string(),
        };
        let request = tonic::Request::new(append_records_req);
        client.append_records(request).await?;
        Ok(())
    }

    #[inline]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}
