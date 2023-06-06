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
use db3_crypto::id::DbId;
use db3_error::Result;
use db3_proto::db3_storage_proto::{
    storage_node_server::StorageNode, ExtraItem, GetNonceRequest, GetNonceResponse,
    SendMutationRequest, SendMutationResponse,
};

use db3_proto::db3_mutation_v2_proto::MutationAction;
use db3_storage::mutation_store::{MutationStore, MutationStoreConfig};
use db3_storage::state_store::{StateStore, StateStoreConfig};
use tonic::{Request, Response, Status};
use tracing::debug;

pub struct StorageNodeV2Config {
    pub store_config: MutationStoreConfig,
    pub state_config: StateStoreConfig,
    pub network_id: u64,
}

pub struct StorageNodeV2Impl {
    storage: MutationStore,
    state_store: StateStore,
    config: StorageNodeV2Config,
}

impl StorageNodeV2Impl {
    pub fn new(config: StorageNodeV2Config) -> Result<Self> {
        let storage = MutationStore::new(config.store_config.clone())?;
        let state_store = StateStore::new(config.state_config.clone())?;
        Ok(Self {
            storage,
            state_store,
            config,
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
        // validate the signature
        let (dm, address, nonce) =
            MutationUtil::unwrap_and_light_verify(&r.payload, r.signature.as_str())
                .map_err(|e| Status::internal(format!("{e}")))?;
        let action = MutationAction::from_i32(dm.action)
            .ok_or(Status::internal("fail to convert action type".to_string()))?;
        // TODO validate the database mutation
        match self.state_store.incr_nonce(&address, nonce) {
            Ok(_) => {
                // mutation id
                let id = self
                    .storage
                    .add_mutation(&r.payload, r.signature.as_bytes(), &address)
                    .map_err(|e| Status::internal(format!("{e}")))?;
                match action {
                    MutationAction::CreateDocumentDb => {
                        let db_addr =
                            DbId::from((&address, nonce, self.config.network_id)).to_hex();
                        let item = ExtraItem {
                            key: "db_addr".to_string(),
                            value: db_addr,
                        };
                        Ok(Response::new(SendMutationResponse {
                            id,
                            code: 0,
                            msg: "ok".to_string(),
                            items: vec![item],
                        }))
                    }
                    _ => Ok(Response::new(SendMutationResponse {
                        id,
                        code: 0,
                        msg: "ok".to_string(),
                        items: vec![],
                    })),
                }
            }
            Err(_e) => Ok(Response::new(SendMutationResponse {
                id: "".to_string(),
                code: 1,
                msg: "bad nonce".to_string(),
                items: vec![],
            })),
        }
    }
}
