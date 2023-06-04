//
// stroage_node_light_impl.rs
// Copyright (C) 2023 db3.network Author imotai <codego.me@gmail.com>
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
use db3_proto::db3_storage_proto::{
    storage_node_server::StorageNode, SendMutationRequest, SendMutationResponse,
};

use db3_storage::mutation_store::{MutationStore, MutationStoreConfig};
use tonic::{Request, Response, Status};

pub struct StorageNodeV2Config {
    pub store_config: MutationStoreConfig,
}

pub struct StorageNodeV2Impl {
    storage: MutationStore,
}

#[tonic::async_trait]
impl StorageNode for StorageNodeV2Impl {
    async fn send_mutation(
        &self,
        request: Request<SendMutationRequest>,
    ) -> std::result::Result<Response<SendMutationResponse>, Status> {
        let r = request.into_inner();
        let id = self
            .storage
            .add_mutation(&r.payload, &r.signature)
            .map_err(|e| Status::internal(format!("{e}")))?;

        Ok(Response::new(SendMutationResponse {
            id,
            code: 0,
            msg: "ok".to_string(),
        }))
    }
}
