//
// stroage_node_impl.rs
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

use super::auth_storage::AuthStorage;
use db3_crypto::verifier::Verifier;
use db3_proto::db3_node_proto::{
    storage_node_server::StorageNode, BatchGetKey, GetKeyRequest, GetKeyResponse, QueryBillRequest,
    QueryBillResponse,
};
use prost::Message;
use std::boxed::Box;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

pub struct StorageNodeImpl {
    store: Arc<Mutex<Pin<Box<AuthStorage>>>>,
}

impl StorageNodeImpl {
    pub fn new(store: Arc<Mutex<Pin<Box<AuthStorage>>>>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl StorageNode for StorageNodeImpl {
    async fn query_bill(
        &self,
        request: Request<QueryBillRequest>,
    ) -> std::result::Result<Response<QueryBillResponse>, Status> {
        let r = request.into_inner();
        match self.store.lock() {
            Ok(s) => {
                let bills = s
                    .get_bills(r.height, r.start_id, r.end_id)
                    .map_err(|e| Status::internal(format!("{}", e)))?;
                Ok(Response::new(QueryBillResponse { bills }))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }

    async fn get_key(
        &self,
        request: Request<GetKeyRequest>,
    ) -> std::result::Result<Response<GetKeyResponse>, Status> {
        let r = request.into_inner();
        let account_id = Verifier::verify(r.batch_get.as_ref(), r.signature.as_ref())
            .map_err(|e| Status::internal(format!("{}", e)))?;
        let batch_get_key = BatchGetKey::decode(r.batch_get.as_ref())
            .map_err(|_| Status::internal("fail to decode batch get key".to_string()))?;
        match self.store.lock() {
            Ok(s) => {
                let values = s
                    .batch_get(&account_id.addr, &batch_get_key)
                    .map_err(|e| Status::internal(format!("{}", e)))?;
                Ok(Response::new(GetKeyResponse {
                    signature: vec![],
                    batch_get_values: Some(values.to_owned()),
                }))
            }
            Err(e) => Err(Status::internal(format!("{}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
