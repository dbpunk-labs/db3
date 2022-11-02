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
use db3_proto::db3_node_proto::{
    storage_node_server::StorageNode, QueryBillRequest, QueryBillResponse,
};
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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {}
}
