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
use crate::catalog::catalog::Catalog;
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
    memory_sdk: MemoryNodeSDK,
    catalog: Arc<Catalog>,
}

unsafe impl Send for SQLExecutor {}
unsafe impl Sync for SQLExecutor {}

impl SQLExecutor {
    pub fn new(
        meta_sdk: MetaNodeSDK,
        meta_store: Arc<MetaStore>,
        memory_sdk: MemoryNodeSDK,
    ) -> Self {
        let catalog = Arc::new(Catalog::new(meta_store));
        Self {
            meta_sdk,
            memory_sdk,
            catalog,
        }
    }

    pub async fn init(&self) -> Result<()> {
        self.catalog.recover().await?;
        Catalog::subscribe_changes(&self.catalog).await;
        Ok(())
    }

    pub fn parse_sql(sql: &str) -> Result<(Keyword, SQLStatement)> {
        let dialect = MySqlDialect {};
        let mut parser = InterruptibleParser::new(&dialect, sql)?;
        let keyword = parser.next_keyword()?;
        parser.prev_token();
        let statement = parser.parse_left()?;
        Ok((keyword, statement))
    }

    async fn handle_insert(&self, db: &str, table_name: &str, expr: &SetExpr) -> Result<()> {
        let database = self.catalog.get_db(db)?;
        let table = database.get_table(table_name)?;
        if let SetExpr::Values(values) = expr {
            if let Some(schema) = &table.get_table_desc().schema {
                let row_batch = mysql_utils::sql_to_row_batch(&schema, &values.0[0])?;
                //TODO add logical for partition
                if self
                    .memory_sdk
                    .append_records(db, table_name, 0, &row_batch)
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
        db: &str,
        table_name: &str,
        columns: &Vec<ColumnDef>,
    ) -> Result<()> {
        let schema_desc = mysql_utils::sql_to_table_desc(columns)?;
        let table_desc = RtStoreTableDesc {
            name: table_name.to_string(),
            schema: Some(schema_desc),
            partition_desc: None,
            db: db.to_string(),
            ctime: 0,
        };
        if let Err(e) = self.meta_sdk.create_table(table_desc).await {
            warn!("fail  to create table for err {}", e);
        }
        Ok(())
    }

    async fn handle_create_db(&self, db: &str) -> Result<()> {
        if let Err(e) = self.meta_sdk.create_db(db).await {
            warn!("fail to create db for err {}", e);
        }
        Ok(())
    }

    pub async fn execute(&self, sql: &str, db: Option<String>) -> Result<SQLResult> {
        debug!("input sql {}", sql);
        let (keyword, statement) = Self::parse_sql(sql)?;
        match (keyword, statement, db) {
            (Keyword::CREATE, SQLStatement::CreateTable { name, columns, .. }, Some(db_str)) => {
                self.handle_create_table(&db_str, &name.0[0].value, &columns)
                    .await?;
                Ok(SQLResult { effected_rows: 1 })
            }
            (Keyword::CREATE, SQLStatement::CreateDatabase { db_name, .. }, _) => {
                self.handle_create_db(&db_name.0[0].value).await?;
                Ok(SQLResult { effected_rows: 1 })
            }
            (
                Keyword::INSERT,
                SQLStatement::Insert {
                    table_name, source, ..
                },
                Some(db_str),
            ) => {
                self.handle_insert(&db_str, &table_name.0[0].value, &source.body)
                    .await?;
                Ok(SQLResult { effected_rows: 1 })
            }

            (_, _, _) => {
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
