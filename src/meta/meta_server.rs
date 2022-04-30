//
//
// meta_server.rs
// Copyright (C) 2022 rtstore.io Author imrtstore <rtstore_dev@outlook.com>
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
use super::table::Table;
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::RtStoreTableDesc;
use crate::proto::rtstore_meta_proto::meta_server::Meta;
use crate::proto::rtstore_meta_proto::{
    CreateTableRequest, CreateTableResponse, PingRequest, PingResponse,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
uselog!(debug, info, warn);

pub struct MetaServiceState {
    // key is the id of table
    tables: HashMap<String, Table>,
}

impl MetaServiceState {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, table_desc: &RtStoreTableDesc) -> Result<()> {
        // join the names of table desc
        let id = Table::gen_id(table_desc)?;
        debug!("create table with id {}", id);
        match self.tables.get(&id) {
            Some(_) => Err(RTStoreError::TableNamesExistError { name: id }),
            _ => {
                let table = Table::new(table_desc)?;
                info!("create a new table with id {} successfully", id);
                self.tables.insert(id, table);
                Ok(())
            }
        }
    }
}

impl Default for MetaServiceState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct MetaServiceImpl {
    state: Arc<Mutex<MetaServiceState>>,
}

impl Default for MetaServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaServiceImpl {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MetaServiceState::new())),
        }
    }
}

impl From<RTStoreError> for Status {
    fn from(error: RTStoreError) -> Self {
        match error {
            RTStoreError::TableInvalidNamesError { .. }
            | RTStoreError::TableSchemaConvertError { .. }
            | RTStoreError::TableSchemaInvalidError { .. }
            | RTStoreError::MetaRpcCreateTableError { .. } => Status::invalid_argument(error),
            RTStoreError::TableNotFoundError { .. } => Status::not_found(error),
            RTStoreError::FSInvalidFileError { .. } | RTStoreError::FSIoError(_) => {
                Status::internal(error)
            }
            RTStoreError::TableNamesExistError { .. } => Status::already_exists(error),
        }
    }
}

#[tonic::async_trait]
impl Meta for MetaServiceImpl {
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> std::result::Result<Response<CreateTableResponse>, Status> {
        let create_request = request.into_inner();
        let table_desc = match &create_request.table_desc {
            Some(t) => Ok(t),
            _ => Err(RTStoreError::MetaRpcCreateTableError {
                err: "input is invalid for empty table description".to_string(),
            }),
        }?;
        let mut local_state = self.state.lock().unwrap();
        local_state.create_table(table_desc)?;
        Ok(Response::new(CreateTableResponse {}))
    }

    async fn ping(
        &self,
        request: Request<PingRequest>,
    ) -> std::result::Result<Response<PingResponse>, Status> {
        Ok(Response::new(PingResponse {}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {}
}
