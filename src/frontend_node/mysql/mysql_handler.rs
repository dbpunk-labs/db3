//
//
// mysql_handler.rs
// Copyright (C) 2022 Author zombie <zombie@zombie-ub2104>
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

use super::sql_handler::SQLExecutor;
use crate::base::mysql_utils;
use crate::error::Result as RtStoreResult;
use crate::sdk::compute_node_sdk::ComputeNodeSDK;
use crate::sdk::memory_node_sdk::MemoryNodeSDK;
use crate::sdk::meta_node_sdk::MetaNodeSDK;
use crate::store::meta_store::MetaStore;
use async_trait::async_trait;
use msql_srv::AsyncMysqlShim;
use msql_srv::InitWriter;
use msql_srv::OkResponse;
use msql_srv::ParamParser;
use msql_srv::QueryResultWriter;
use msql_srv::StatementMetaWriter;
use rand::RngCore;
use std::io::{Error, Result};
use std::sync::Arc;
uselog!(debug, info, warn);

pub struct MySQLHandler {
    version: String,
    id: u32,
    salt: [u8; 20],
    sql_executor: SQLExecutor,
    db: Option<String>,
}

impl Clone for MySQLHandler {
    fn clone(&self) -> Self {
        let mut bs = vec![0u8; 20];
        let mut rng = rand::thread_rng();
        rng.fill_bytes(bs.as_mut());
        let mut scramble: [u8; 20] = [0; 20];
        for i in 0..20 {
            scramble[i] = bs[i];
            if scramble[i] == b'\0' || scramble[i] == b'$' {
                scramble[i] += 1;
            }
        }
        Self {
            version: self.version.clone(),
            salt: scramble,
            id: self.id + 1,
            sql_executor: self.sql_executor.clone(),
            db: self.db.clone(),
        }
    }
}

impl MySQLHandler {
    pub fn new(
        meta_sdk: MetaNodeSDK,
        memory_sdk: MemoryNodeSDK,
        compute_sdk: ComputeNodeSDK,
        meta_store: Arc<MetaStore>,
        var_config_path: &str,
    ) -> Result<Self> {
        Ok(Self {
            version: "8.0.26-rtstore".to_string(),
            id: 0,
            salt: [0 as u8; 20],
            sql_executor: SQLExecutor::new(
                meta_sdk,
                meta_store,
                memory_sdk,
                compute_sdk,
                var_config_path,
            )?,
            db: None,
        })
    }
    pub async fn init(&self) -> RtStoreResult<()> {
        self.sql_executor.init().await
    }
}

#[async_trait]
impl<W: std::io::Write + Send> AsyncMysqlShim<W> for MySQLHandler {
    type Error = Error;

    fn version(&self) -> &str {
        self.version.as_str()
    }

    fn connect_id(&self) -> u32 {
        self.id
    }

    fn default_auth_plugin(&self) -> &str {
        "mysql_native_password"
    }

    fn auth_plugin_for_username(&self, _user: &[u8]) -> &str {
        "mysql_native_password"
    }

    fn salt(&self) -> [u8; 20] {
        self.salt
    }

    async fn authenticate(
        &self,
        _auth_plugin: &str,
        _username: &[u8],
        _salt: &[u8],
        _auth_data: &[u8],
    ) -> bool {
        true
    }

    async fn authenticate_with_db(
        &self,
        _auth_plugin: &str,
        _username: &[u8],
        _salt: &[u8],
        _auth_data: &[u8],
        _db: &[u8],
    ) -> bool {
        true
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        _writer: StatementMetaWriter<'a, W>,
    ) -> Result<()> {
        info!("on prepare query {}", query);
        Ok(())
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        _param: ParamParser<'a>,
        _writer: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        info!("on exec id {}", id);
        Ok(())
    }

    async fn on_close<'a>(&'a mut self, id: u32)
    where
        W: 'async_trait,
    {
        info!("on close id {}", id);
    }

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> Result<()> {
        info!("execute {} ", sql);
        if let Ok(result) = self.sql_executor.execute(sql, &self.db).await {
            if let Some(batches) = result.batch {
                mysql_utils::write_batch_to_resultset(&batches, results).unwrap();
            } else {
                results.completed(OkResponse::default())?;
            }
        } else {
            results.completed(OkResponse::default())?;
        }
        Ok(())
    }

    async fn on_init<'a>(
        &'a mut self,
        database_name: &'a str,
        writer: InitWriter<'a, W>,
    ) -> Result<()> {
        self.db = Some(database_name.to_string());
        info!("enter db {}", database_name);
        writer.ok()
    }
}
