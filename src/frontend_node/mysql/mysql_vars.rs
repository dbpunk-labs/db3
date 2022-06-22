//
//
// mysql_vars.rs
// Copyright (C) 2022 peasdb.ai Author imotai <codego.me@gmail.com>
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
use crate::base::{arrow_parquet_utils, linked_list::LinkedList};
use crate::codec::row_codec::{Data, RowRecordBatch};
use crate::error::Result;
use arrow::datatypes::{DataType, Field as ArrowField, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::fs::File;
use std::io::*;
use std::sync::Arc;

uselog!(debug, info, warn);

pub struct MySQLVars {
    global_sys_vars: HashMap<String, String>,
    session_sys_vars: HashMap<String, String>,
}

impl MySQLVars {
    pub fn new(config_path: &str) -> Result<Self> {
        let fd = File::open(config_path).unwrap();
        let reader = BufReader::new(fd);
        let mut vars: HashMap<String, String> = HashMap::new();
        for line_ret in reader.lines() {
            let line = line_ret.unwrap();
            let kv: Vec<&str> = line.split('\t').collect();
            debug!("input line {} and split len {}", line, kv.len());
            if kv.len() != 2 {
                warn!("invalid line format {}", line);
                continue;
            }
            match kv[1] {
                "ON" => {
                    vars.insert(kv[0].to_string(), "1".to_string());
                }
                "OFF" => {
                    vars.insert(kv[0].to_string(), "0".to_string());
                }
                _ => {
                    vars.insert(kv[0].to_string(), kv[1].to_string());
                }
            }
        }
        Ok(MySQLVars {
            global_sys_vars: vars.clone(),
            session_sys_vars: vars.clone(),
        })
    }

    pub fn build_select_output(
        &self,
        projection: &Vec<(String, bool, String)>,
    ) -> Result<RecordBatch> {
        let mut schema_vec: Vec<ArrowField> = Vec::new();
        let mut row: Vec<Data> = Vec::new();
        for (key, is_session, alias) in projection {
            schema_vec.push(ArrowField::new(alias, DataType::Utf8, false));
            match is_session {
                true => match self.session_sys_vars.get(key) {
                    Some(v) => {
                        row.push(Data::Varchar(v.to_string()));
                    }
                    _ => {
                        row.push(Data::Varchar("Not Found".to_string()));
                    }
                },
                _ => match self.global_sys_vars.get(key) {
                    Some(v) => {
                        row.push(Data::Varchar(v.to_string()));
                    }
                    _ => {
                        row.push(Data::Varchar("Not Found".to_string()));
                    }
                },
            }
        }
        let schema = Arc::new(Schema::new(schema_vec));
        let rows = RowRecordBatch {
            batch: vec![row],
            schema_version: 0,
        };
        let data = LinkedList::<RowRecordBatch>::new();
        data.push_front(rows)?;
        arrow_parquet_utils::rows_to_columns(&schema, &data)
    }

    pub fn build_show_output(
        &self,
        projection: &Vec<(String, bool, String)>,
    ) -> Result<RecordBatch> {
        let schema_vec = vec![
            ArrowField::new("Variable_name", DataType::Utf8, false),
            ArrowField::new("Value", DataType::Utf8, false),
        ];
        let mut rows: Vec<Vec<Data>> = Vec::new();
        for (key, is_session, alias) in projection {
            let mut row = vec![Data::Varchar(alias.to_string())];
            match is_session {
                true => match self.session_sys_vars.get(key) {
                    Some(v) => {
                        row.push(Data::Varchar(v.to_string()));
                    }
                    _ => {
                        row.push(Data::Varchar("Not Found".to_string()));
                    }
                },
                _ => match self.global_sys_vars.get(key) {
                    Some(v) => {
                        row.push(Data::Varchar(v.to_string()));
                    }
                    _ => {
                        row.push(Data::Varchar("Not Found".to_string()));
                    }
                },
            }
            rows.push(row);
        }
        let schema = Arc::new(Schema::new(schema_vec));
        let batch = RowRecordBatch {
            batch: rows,
            schema_version: 0,
        };
        let data = LinkedList::<RowRecordBatch>::new();
        data.push_front(batch)?;
        arrow_parquet_utils::rows_to_columns(&schema, &data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_test_load_config() -> Result<()> {
        let path: &str = "./static/vars.txt";
        let mysql_vars = MySQLVars::new(path)?;
        assert_eq!(
            true,
            mysql_vars.global_sys_vars.contains_key("wait_timeout")
        );
        Ok(())
    }
}
