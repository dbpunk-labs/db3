//
//
// sql_engine.rs
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

uselog!(debug, info, warn);
use crate::catalog::catalog::Catalog;
use crate::error::Result;
use crate::frontend_node::mysql::interruptible_parser::*;
use arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use datafusion::sql::planner::SqlToRel;
use sqlparser::{
    ast::{ColumnDef, SetExpr, Statement as SQLStatement},
    dialect::{keywords::Keyword, MySqlDialect},
};
use std::sync::Arc;

pub struct SQLResult {
    pub batch: Option<Vec<RecordBatch>>,
    pub effected_rows: usize,
}

pub struct SQLEngine {
    catalog: Arc<Catalog>,
    runtime: Arc<RuntimeEnv>,
}

impl SQLEngine {
    pub fn new(catalog: &Arc<Catalog>, runtime: &Arc<RuntimeEnv>) -> Self {
        Self {
            catalog: catalog.clone(),
            runtime: runtime.clone(),
        }
    }
    fn parse_sql(sql: &str) -> Result<(Keyword, SQLStatement)> {
        let dialect = MySqlDialect {};
        let mut parser = InterruptibleParser::new(&dialect, sql)?;
        let keyword = parser.next_keyword()?;
        parser.prev_token();
        let statement = parser.parse_left()?;
        Ok((keyword, statement))
    }

    pub async fn execute(&self, sql: &str, db: Option<String>) -> Result<SQLResult> {
        let (_, statement) = Self::parse_sql(sql)?;
        let config = match db {
            Some(name) => {
                let config = SessionConfig::new();
                let config = config.with_information_schema(true);
                config.with_default_catalog_and_schema("rtstore", name)
            }
            _ => {
                let config = SessionConfig::new();
                let config = config.with_information_schema(true);
                config.with_default_catalog_and_schema("rtstore", "public")
            }
        };
        //TODO use session id to cache session context
        let stx = SessionContext::with_config_rt(config, self.runtime.clone());
        stx.register_catalog("rtstore", self.catalog.clone());
        let state = stx.state.read().clone();
        let query_planner = SqlToRel::new(&state);
        let plan = query_planner.sql_statement_to_plan(statement)?;
        let opt_plan = stx.optimize(&plan)?;
        let ret = Arc::new(DataFrame::new(stx.state.clone(), &opt_plan));
        // use streaming resultset
        let batches = ret.collect().await?;
        Ok(SQLResult {
            batch: Some(batches),
            effected_rows: 0,
        })
    }
}
