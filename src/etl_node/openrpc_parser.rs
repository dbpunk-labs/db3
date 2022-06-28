//
//
// openrpc_parser.rs
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


use crate::error::Result;
use serde_json::{Deserializer, Value};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

pub fn parse_openrpc(path: &str) -> Result<Value> {
    let file_path = Path::new(path);
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let value: Value = serde_json::from_reader(reader)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse() -> Result<()> {
        let path: &str = "./static/openrpc.json";
        let v = parse_openrpc(path)?;
        println!("parse {}", v["methods"][0]["name"]);
        Ok(())
    }
}
