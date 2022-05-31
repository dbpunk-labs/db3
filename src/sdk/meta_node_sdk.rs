//
//
// meta_node_sdk.rs
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
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{RtStoreNodeType, RtStoreTableDesc};
use crate::proto::rtstore_meta_proto::meta_client::MetaClient;
use crate::proto::rtstore_meta_proto::{CreateTableRequest, CreateTableResponse};
use std::sync::Arc;
use tonic::{Request, Response, Status};
uselog!(info);

pub struct MetaNodeSDK {
    endpoint: String,
    // clone on use
    client: Arc<MetaClient<tonic::transport::Channel>>,
}

impl Clone for MetaNodeSDK {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.to_string(),
            client: self.client.clone(),
        }
    }
}

impl MetaNodeSDK {
    pub async fn connect(endpoint: &str) -> std::result::Result<Self, tonic::transport::Error> {
        // create a new client connection
        let client = Arc::new(MetaClient::connect(endpoint.to_string()).await?);
        Ok(MetaNodeSDK {
            endpoint: endpoint.to_string(),
            client,
        })
    }

    pub async fn create_table(&self, table: RtStoreTableDesc) -> std::result::Result<(), Status> {
        let mut client = self.client.as_ref().clone();
        let create_table_req = CreateTableRequest {
            table_desc: Some(table),
        };
        let request = tonic::Request::new(create_table_req);
        client.create_table(request).await?;
        Ok(())
    }
}
