//
//
// sql_handler.rs
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

use super::interruptible_parser::*;
use crate::base::mysql_utils;
use crate::proto::rtstore_base_proto::{RtStoreNodeType, RtStoreTableDesc};
use crate::store::meta_store::MetaStore;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use sqlparser::{
    ast::{ColumnDef, SetExpr, Statement as SQLStatement},
    dialect::{keywords::Keyword, MySqlDialect},
};
use std::sync::{Arc, Mutex};
uselog!(debug, info, warn);
use crate::error::{RTStoreError, Result};
use crate::sdk::memory_node_sdk::MemoryNodeSDK;
use crate::sdk::meta_node_sdk::MetaNodeSDK;
use parquet::record::Field;

pub struct SQLResult {
    pub effected_rows: usize,
}

#[derive(Clone)]
pub struct SQLExecutor {
    meta_sdk: MetaNodeSDK,
    meta_store: Arc<MetaStore>,
    memory_sdk: MemoryNodeSDK,
}

unsafe impl Send for SQLExecutor {}
unsafe impl Sync for SQLExecutor {}

impl SQLExecutor {
    pub fn new(
        meta_sdk: MetaNodeSDK,
        meta_store: Arc<MetaStore>,
        memory_sdk: MemoryNodeSDK,
    ) -> Self {
        Self {
            meta_sdk,
            meta_store,
            memory_sdk,
        }
    }

    pub fn parse_sql(sql: &str) -> Result<(Keyword, SQLStatement)> {
        let dialect = MySqlDialect {};
        let mut parser = InterruptibleParser::new(&dialect, sql)?;
        let keyword = parser.next_keyword()?;
        parser.prev_token();
        let statement = parser.parse_left()?;
        Ok((keyword, statement))
    }

    fn build_full_name(&self, table_name: &str, db: Option<String>) -> Vec<String> {
        if let Some(d) = db {
            vec![d, table_name.to_string()]
        } else {
            vec![table_name.to_string()]
        }
    }

    async fn handle_insert(&self, table_name: &str, expr: &SetExpr) -> Result<()> {
        let table_desc_opt = self.meta_store.get_table_desc(table_name);
        if let (Some(table_desc), SetExpr::Values(values)) = (table_desc_opt, expr) {
            if let Some(schema) = &table_desc.schema {
                let row_batch = mysql_utils::sql_to_row_batch(table_name, &schema, &values.0[0])?;
                if self
                    .memory_sdk
                    .append_records(table_name, 0, &row_batch)
                    .await
                    .is_err()
                {
                    warn!("fail to append record to table {}", table_name);
                } else {
                    debug!("insert into table {} ok", table_name);
                }
            }
        } else {
            warn!("table with name {} not exist", table_name);
        }
        Ok(())
    }

    async fn handle_create_table(
        &self,
        table_full_name: Vec<String>,
        columns: &Vec<ColumnDef>,
    ) -> Result<()> {
        let schema_desc = mysql_utils::sql_to_table_desc(columns)?;
        let table_desc = RtStoreTableDesc {
            names: table_full_name,
            schema: Some(schema_desc),
            partition_desc: None,
        };
        if let Err(e) = self.meta_sdk.create_table(table_desc).await {
            warn!("fail  to create table for err {}", e);
        }
        Ok(())
    }

    pub async fn execute(&self, sql: &str, db: Option<String>) -> Result<SQLResult> {
        debug!("input sql {}", sql);
        let (keyword, statement) = Self::parse_sql(sql)?;
        match (keyword, statement) {
            (Keyword::CREATE, SQLStatement::CreateTable { name, columns, .. }) => {
                let table_full_name = self.build_full_name(&name.0[0].value, db);
                self.handle_create_table(table_full_name, &columns).await?;
                Ok(SQLResult { effected_rows: 1 })
            }
            (
                Keyword::INSERT,
                SQLStatement::Insert {
                    table_name, source, ..
                },
            ) => {
                let table_full_name = self.build_full_name(&table_name.0[0].value, db).join(".");
                debug!("process sql insert for table {} ", table_full_name);
                self.handle_insert(&table_full_name, &source.body).await?;
                Ok(SQLResult { effected_rows: 1 })
            }

            (_, _) => {
                warn!("sql {} is not handled", sql);
                Ok(SQLResult { effected_rows: 0 })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::mysql_utils;
    use crate::error::Result;
}
