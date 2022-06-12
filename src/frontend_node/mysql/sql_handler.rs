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
use super::mysql_vars::MySQLVars;
use crate::base::{arrow_parquet_utils, linked_list::LinkedList, mysql_utils};
use crate::codec::flight_codec::flight_data_to_arrow_batch;
use crate::codec::row_codec::{Data, RowRecordBatch};
use crate::proto::rtstore_base_proto::{RtStoreNodeType, RtStoreTableDesc};
use crate::store::meta_store::MetaStore;
use arrow::datatypes::{DataType, Field as ArrowField};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::schema::SchemaProvider;
use sqlparser::{
    ast::{ColumnDef, Expr, Ident, SelectItem, SetExpr, Statement as SQLStatement, UnaryOperator},
    dialect::{keywords::Keyword, MySqlDialect},
};
use std::sync::Arc;
uselog!(debug, info, warn);
use crate::catalog::catalog::Catalog;
use crate::error::{RTStoreError, Result};
use crate::sdk::compute_node_sdk::ComputeNodeSDK;
use crate::sdk::memory_node_sdk::MemoryNodeSDK;
use crate::sdk::meta_node_sdk::MetaNodeSDK;
use arrow::datatypes::{Schema, SchemaRef};
use parquet::record::Field;
use regex::RegexSet;
use std::collections::HashMap;

pub struct SQLResult {
    pub batch: Option<Vec<RecordBatch>>,
    pub effected_rows: usize,
}

#[derive(Clone)]
pub struct SQLExecutor {
    meta_sdk: MetaNodeSDK,
    memory_sdk: MemoryNodeSDK,
    compute_sdk: ComputeNodeSDK,
    catalog: Arc<Catalog>,
    system_vars: Arc<MySQLVars>,
}

unsafe impl Send for SQLExecutor {}
unsafe impl Sync for SQLExecutor {}

impl SQLExecutor {
    pub fn new(
        meta_sdk: MetaNodeSDK,
        meta_store: Arc<MetaStore>,
        memory_sdk: MemoryNodeSDK,
        compute_sdk: ComputeNodeSDK,
        var_config_path: &str,
    ) -> Result<Self> {
        let catalog = Arc::new(Catalog::new(meta_store));
        let system_vars = Arc::new(MySQLVars::new(var_config_path)?);
        Ok(Self {
            meta_sdk,
            memory_sdk,
            compute_sdk,
            catalog,
            system_vars,
        })
    }

    pub async fn init(&self) -> Result<()> {
        self.catalog.recover().await?;
        Catalog::subscribe_changes(&self.catalog).await;
        Ok(())
    }

    fn is_query_system_vars(&self, query: &SetExpr) -> bool {
        if let SetExpr::Select(s) = &query {
            let mut all_is_sys_vars = !s.projection.is_empty();
            for s in &s.projection {
                match s {
                    SelectItem::UnnamedExpr(Expr::UnaryOp {
                        op: UnaryOperator::DoubleAt,
                        ..
                    }) => {
                        all_is_sys_vars &= true;
                    }
                    SelectItem::ExprWithAlias {
                        expr:
                            Expr::UnaryOp {
                                op: UnaryOperator::DoubleAt,
                                ..
                            },
                        ..
                    } => {
                        all_is_sys_vars &= true;
                    }
                    _ => {
                        all_is_sys_vars &= false;
                    }
                }
            }
            return all_is_sys_vars;
        }
        false
    }

    fn parse_sql(sql: &str) -> Result<(Keyword, SQLStatement)> {
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
                let row_batch = mysql_utils::sql_to_row_batch(schema, &values.0[0])?;
                //TODO add logical for partition
                if let Err(e) = self
                    .memory_sdk
                    .append_records(db, table_name, 0, &row_batch)
                    .await
                {
                    warn!("fail to append record to table {} for {}", table_name, e);
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
    fn direct_return_for_mysql(&self, sql: &str) -> bool {
        let expr = RegexSet::new(&[
            "(?i)^(SET NAMES(.*))",
            "(?i)^(SET character_set_results(.*))",
            "(?i)^(SET FOREIGN_KEY_CHECKS(.*))",
            "(?i)^(SET AUTOCOMMIT(.*))",
            "(?i)^(SET sql_mode(.*))",
            "(?i)^(SET @@(.*))",
            "(?i)^(SET SESSION TRANSACTION ISOLATION LEVEL(.*))",
            // Just compatibility for jdbc
            "(?i)^(/\\* mysql-connector-java(.*))",
        ])
        .unwrap();
        expr.is_match(sql)
    }
    async fn handle_create_db(&self, db: &str) -> Result<()> {
        if let Err(e) = self.meta_sdk.create_db(db).await {
            warn!("fail to create db for err {}", e);
        } else {
            info!("create database {} ok", db);
        }
        Ok(())
    }
    async fn handle_query(&self, sql: &str, db: &Option<String>) -> Result<SQLResult> {
        let db_str = db.clone().map_or("".to_string(), |v| v);
        match self.compute_sdk.query(sql, &db_str).await {
            Ok(resp) => {
                let mut stream = resp.into_inner();
                let flight_data = stream.message().await?.unwrap();
                // convert FlightData to a stream
                let schema: SchemaRef = Arc::new(Schema::try_from(&flight_data)?);
                let mut results = vec![];
                let dictionaries_by_field = HashMap::new();
                while let Some(flight_data) = stream.message().await? {
                    let record_batch = flight_data_to_arrow_batch(
                        &flight_data,
                        schema.clone(),
                        &dictionaries_by_field,
                    )?;
                    results.push(record_batch);
                }
                info!("call compute node done with schema {}", schema);
                Ok(SQLResult {
                    batch: Some(results),
                    effected_rows: 0,
                })
            }
            Err(e) => {
                info!("fail to call compute node for e {}", e);
                // add error handle
                Ok(SQLResult {
                    batch: None,
                    effected_rows: 0,
                })
            }
        }
    }

    fn handle_show_create_table(&self, db: &str, tname: &str) -> Result<SQLResult> {
        let database = self.catalog.get_db(db)?;
        let table = database.get_table(tname)?;
        let batch = arrow_parquet_utils::schema_to_ddl_recordbatch(tname, table.get_schema())?;
        Ok(SQLResult {
            batch: Some(vec![batch]),
            effected_rows: 0,
        })
    }

    fn handle_select_variable(&self, query_body: &SetExpr) -> Result<SQLResult> {
        if let SetExpr::Select(s) = query_body {
            let mut query: Vec<(String, bool, String)> = Vec::new();
            for p in &s.projection {
                match p {
                    SelectItem::UnnamedExpr(expr) => match expr {
                        Expr::UnaryOp { op: _, expr } => match expr.as_ref() {
                            //session.transaction_read_only
                            Expr::CompoundIdentifier(idents) => {
                                let v1 = &idents[0].value;
                                let v2 = &idents[1].value;
                                if v1.eq("session") {
                                    query.push((v2.to_string(), true, v2.to_string()));
                                } else {
                                    query.push((v2.to_string(), false, v2.to_string()));
                                }
                            }
                            // transaction_read_only
                            Expr::Identifier(id) => {
                                let value = &id.value;
                                query.push((value.to_string(), false, value.to_string()));
                            }
                            _ => {
                                warn!("unsupported expr {}", expr);
                            }
                        },
                        _ => {
                            warn!("unsupported expr {}", expr);
                        }
                    },
                    SelectItem::ExprWithAlias { expr, alias } => match expr {
                        Expr::UnaryOp { op: _, expr } => match expr.as_ref() {
                            //session.transaction_read_only
                            Expr::CompoundIdentifier(idents) => {
                                let v1 = &idents[0].value;
                                let v2 = &idents[1].value;
                                if v1.eq("session") {
                                    query.push((v2.to_string(), true, alias.value.to_string()));
                                } else {
                                    query.push((v2.to_string(), false, alias.value.to_string()));
                                }
                            }
                            // transaction_read_only
                            Expr::Identifier(id) => {
                                query.push((id.value.to_string(), false, alias.value.to_string()));
                            }
                            _ => {
                                warn!("unsupported expr {}", expr);
                            }
                        },
                        _ => {
                            warn!("unsupported expr {}", expr);
                        }
                    },
                    _ => {
                        warn!("unsupported projection {}", p);
                    }
                }
            }
            let batch = self.system_vars.build_select_output(&query)?;
            Ok(SQLResult {
                batch: Some(vec![batch]),
                effected_rows: 0,
            })
        } else {
            Ok(SQLResult {
                batch: None,
                effected_rows: 0,
            })
        }
    }

    fn handle_show_dbs(&self) -> Result<SQLResult> {
        let all_db = self.catalog.get_db_names();
        let schema_vec = vec![ArrowField::new("Databases", DataType::Utf8, false)];
        let mut rows: Vec<Vec<Data>> = Vec::new();
        for db in all_db {
            let row = vec![Data::Varchar(db)];
            rows.push(row);
        }
        let schema = Arc::new(Schema::new(schema_vec));
        let batch = RowRecordBatch {
            batch: rows,
            schema_version: 0,
        };
        let data = LinkedList::<RowRecordBatch>::new();
        data.push_front(batch)?;
        let batch = arrow_parquet_utils::rows_to_columns(&schema, &data)?;
        Ok(SQLResult {
            batch: Some(vec![batch]),
            effected_rows: 0,
        })
    }

    fn handle_show_tables(&self, db: &Option<String>) -> Result<SQLResult> {
        let db_str = db.clone().map_or("".to_string(), |v| v);
        info!("show table in db {}", db_str);
        let database = self.catalog.get_db(&db_str)?;
        let table_names = database.table_names();
        let schema_vec = vec![ArrowField::new("Tables", DataType::Utf8, false)];
        let mut rows: Vec<Vec<Data>> = Vec::new();
        for name in table_names {
            info!("table {}", &name);
            let row = vec![Data::Varchar(name)];
            rows.push(row);
        }
        let schema = Arc::new(Schema::new(schema_vec));
        let batch = RowRecordBatch {
            batch: rows,
            schema_version: 0,
        };
        let data = LinkedList::<RowRecordBatch>::new();
        data.push_front(batch)?;
        let batch = arrow_parquet_utils::rows_to_columns(&schema, &data)?;
        Ok(SQLResult {
            batch: Some(vec![batch]),
            effected_rows: 0,
        })
    }

    fn handle_show_variable(
        &self,
        variable: &Vec<Ident>,
        db: &Option<String>,
    ) -> Result<SQLResult> {
        let mut is_global = true;
        let first_id = &variable[0].value.to_lowercase();
        if first_id.as_str() == "session" {
            is_global = false;
        } else if first_id.as_str() == "databases" {
            // show databases
            return self.handle_show_dbs();
        } else if first_id.as_str() == "tables" {
            return self.handle_show_tables(db);
        }
        let mut query: Vec<(String, bool, String)> = Vec::new();
        for id in variable {
            let lower_id = id.value.to_lowercase();
            match lower_id.as_str() {
                "variables" | "session" | "global" | "in" | "and" | "like" | "where"
                | "variable_name" => {
                    continue;
                }
                _ => {
                    query.push((id.value.to_string(), is_global, id.value.to_string()));
                }
            }
        }
        let batch = self.system_vars.build_show_output(&query)?;
        Ok(SQLResult {
            batch: Some(vec![batch]),
            effected_rows: 0,
        })
    }

    fn handle_desc_table(&self, db: &str, tname: &str) -> Result<SQLResult> {
        let database = self.catalog.get_db(db)?;
        let table = database.get_table(tname)?;
        let batch = arrow_parquet_utils::schema_to_recordbatch(table.get_schema())?;
        Ok(SQLResult {
            batch: Some(vec![batch]),
            effected_rows: 0,
        })
    }

    pub async fn execute(&self, sql: &str, db: &Option<String>) -> Result<SQLResult> {
        if self.direct_return_for_mysql(sql) {
            return Ok(SQLResult {
                batch: None,
                effected_rows: 0,
            });
        }
        debug!("input sql {}", sql);
        let (keyword, statement) = Self::parse_sql(sql)?;
        match (keyword, statement, db) {
            (Keyword::SHOW, SQLStatement::ShowCreate { ref obj_name, .. }, _) => {
                let db_id = &obj_name.0[0];
                let tname_id = &obj_name.0[1];
                self.handle_show_create_table(&db_id.value, &tname_id.value)
            }

            (Keyword::SHOW, SQLStatement::ShowVariable { variable }, _) => {
                self.handle_show_variable(&variable, db)
            }

            (Keyword::CREATE, SQLStatement::CreateTable { name, columns, .. }, Some(db_str)) => {
                self.handle_create_table(db_str, &name.0[0].value, &columns)
                    .await?;
                Ok(SQLResult {
                    batch: None,
                    effected_rows: 1,
                })
            }
            (Keyword::CREATE, SQLStatement::CreateDatabase { db_name, .. }, _) => {
                self.handle_create_db(&db_name.0[0].value).await?;
                Ok(SQLResult {
                    batch: None,
                    effected_rows: 1,
                })
            }
            (
                Keyword::INSERT,
                SQLStatement::Insert {
                    table_name, source, ..
                },
                Some(db_str),
            ) => {
                self.handle_insert(db_str, &table_name.0[0].value, &source.body)
                    .await?;
                Ok(SQLResult {
                    batch: None,
                    effected_rows: 1,
                })
            }
            (Keyword::DESCRIBE, SQLStatement::ExplainTable { table_name, .. }, Some(db_str)) => {
                self.handle_desc_table(db_str, &table_name.0[0].value)
            }
            (_, SQLStatement::Query(q), _) => {
                if self.is_query_system_vars(&q.body) {
                    self.handle_select_variable(&q.body)
                } else {
                    debug!("sql go to compute node");
                    self.handle_query(sql, db).await
                }
            }
            (_, _, _) => {
                debug!("sql go to compute node");
                self.handle_query(sql, db).await
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
