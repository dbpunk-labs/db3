//
//
// row.rs
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

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Data {
    Bool(bool),
    Int8(i8),
    UInt8(u8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Varchar(String),
    Date(u32),
    // time in millsseconds
    Timestamp(u64),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RowRecordBatch {
    batch: Vec<Vec<Data>>,
    schema_version: u32,
    // id for table
    id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_encode() -> Result<(), std::io::Error> {
        let batch = vec![
            vec![Data::Bool(true), Data::Int32(12)],
            vec![Data::Bool(false), Data::Int32(11)],
        ];
        let row_batch = RowRecordBatch {
            batch,
            schema_version: 1,
            id: "eth.price".to_string(),
        };
        let encoded: Vec<u8> = bincode::serialize(&row_batch).unwrap();
        assert_eq!(encoded.len(), 71);
        let new_row_batch: RowRecordBatch = bincode::deserialize(&encoded[..]).unwrap();
        assert_eq!(row_batch.schema_version, new_row_batch.schema_version);
        Ok(())
    }
}
