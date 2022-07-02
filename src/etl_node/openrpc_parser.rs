//
//
// openrpc_parser.rs
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

use crate::error::Result;
use arrow::datatypes::{Schema, SchemaRef};
use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

pub struct JsonRPCTable {
    call_path: Vec<String>,
    object_path: Vec<String>,
    schema: SchemaRef,
    method: Value,
}

pub fn parse_openrpc(path: &str) -> Result<Value> {
    let file_path = Path::new(path);
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let value: Value = serde_json::from_reader(reader)?;
    Ok(value)
}

//pub fn parse_tables(value: &Value) -> Result<Vec<JsonRPCTable>> {
//    let mut tables: Vec<JsonRPCTable> = Vec::new();
//    for v in v["methods"] {
//        let mut call_path: Vec<String> = Vec::new();
//        call_path.push(v["name"]);
//        let mut object_path: Vec<String> = Vec::new();
//    }
//}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::base::test_base::*;

    #[test]
    fn test_parse() -> Result<()> {
        let path: &str = "./chains/eth_openrpc.json";
        let v = parse_openrpc(path)?;
        println!("parse {}", v["methods"][0]["name"]);
        Ok(())
    }
    #[tokio::test]
    async fn test_query_block() -> Result<()> {
        let sql: &str = "select transactions[1]['blockHash'], size from t1;";
        let result = run_sql_on_json("./static/block.json", "t1", sql).await;
        println!("{:?}", result);
        Ok(())
    }
}
