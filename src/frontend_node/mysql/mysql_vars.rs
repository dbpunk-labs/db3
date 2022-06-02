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
use datafusion::error::Result as DResult;
use std::collections::HashMap;
use std::fs::File;
use std::io::*;
uselog!(debug, info, warn);

pub struct MySQLVars {
    pub global_sys_vars: HashMap<String, String>,
    pub session_sys_vars: HashMap<String, String>,
}

impl MySQLVars {
    pub fn new(config_path: &str) -> DResult<Self> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_test_load_config() -> DResult<()> {
        let path: &str = "./static/vars.txt";
        let mysql_vars = MySQLVars::new(path)?;
        assert_eq!(
            true,
            mysql_vars.global_sys_vars.contains_key("wait_timeout")
        );
        Ok(())
    }
}
