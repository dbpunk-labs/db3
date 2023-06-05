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

use crate::mutation_utils::MutationUtil;
use db3_crypto::db3_address::DB3Address;
use db3_error::{DB3Error, Result};
use db3_proto::db3_storage_proto::{
    storage_node_server::StorageNode, GetNonceRequest, GetNonceResponse, SendMutationRequest,
    SendMutationResponse,
};
use db3_storage::mutation_store::{MutationStore, MutationStoreConfig};
use db3_storage::state_store::{StateStore, StateStoreConfig};
use tonic::{Request, Response, Status};
use tracing::{debug, info};

pub struct StorageNodeV2Config {
    pub store_config: MutationStoreConfig,
    pub state_config: StateStoreConfig,
}

pub struct StorageNodeV2Impl {
    storage: MutationStore,
    state_store: StateStore,
}

impl StorageNodeV2Impl {
    pub fn new(store_config: MutationStoreConfig, state_config: StateStoreConfig) -> Result<Self> {
        let storage = MutationStore::new(store_config)?;
        let state_store = StateStore::new(state_config)?;
        Ok(Self {
            storage,
            state_store,
        })
    }
}

#[tonic::async_trait]
impl StorageNode for StorageNodeV2Impl {
    async fn get_nonce(
        &self,
        request: Request<GetNonceRequest>,
    ) -> std::result::Result<Response<GetNonceResponse>, Status> {
        let r = request.into_inner();
        let address = DB3Address::try_from(r.address.as_str())
            .map_err(|e| Status::internal(format!("{e}")))?;
        let used_nonce = self
            .state_store
            .get_nonce(&address)
            .map_err(|e| Status::internal(format!("{e}")))?;
        debug!("address {} used nonce {}", address.to_hex(), used_nonce);
        Ok(Response::new(GetNonceResponse {
            nonce: used_nonce + 1,
        }))
    }

    async fn send_mutation(
        &self,
        request: Request<SendMutationRequest>,
    ) -> std::result::Result<Response<SendMutationResponse>, Status> {
        let r = request.into_inner();
        // validate the request message
        let (_data, _payload_type, account, nonce) =
            MutationUtil::unwrap_and_light_verify(&r.payload, &r.signature)
                .map_err(|e| Status::internal(format!("{e}")))?;
        match self.state_store.incr_nonce(&account.addr, nonce) {
            Ok(_) => {
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
            Err(_e) => Ok(Response::new(SendMutationResponse {
                id: "".to_string(),
                code: 1,
                msg: "bad nonce".to_string(),
            })),
        }
    }
}
