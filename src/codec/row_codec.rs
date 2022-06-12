//
//
// row_codec.rs
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

use crate::error::{RTStoreError, Result};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Data {
    Bool(bool),
    Int8(i8),
    UInt8(u8),
    Int16(i16),
    UInt16(u16),
    Int32(i32),
    Int64(i64),
    UInt64(u64),
    Float(f32),
    Double(f64),
    Varchar(String),
    Date(u32),
    // time in millsseconds
    Timestamp(u64),
}

impl Data {
    pub fn name(&self) -> &'static str {
        match self {
            Data::Bool(_) => "Bool",
            Data::Int8(_) => "Int8",
            Data::UInt8(_) => "UInt8",
            Data::Int16(_) => "Int16",
            Data::UInt16(_) => "UInt16",
            Data::Int32(_) => "Int32",
            Data::Int64(_) => "Int64",
            Data::UInt64(_) => "UInt64",
            Data::Float(_) => "Float",
            Data::Double(_) => "Double",
            Data::Varchar(_) => "Varchar",
            Data::Date(_) => "Date",
            Data::Timestamp(_) => "Timestamp",
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RowRecordBatch {
    pub batch: Vec<Vec<Data>>,
    pub schema_version: u32,
}

pub fn encode(batch: &RowRecordBatch) -> Result<Vec<u8>> {
    match bincode::serialize(batch) {
        Ok(v) => Ok(v),
        Err(e) => Err(RTStoreError::RowCodecError(e)),
    }
}

pub fn decode(data: &[u8]) -> Result<RowRecordBatch> {
    match bincode::deserialize(data) {
        Ok(v) => Ok(v),
        Err(e) => Err(RTStoreError::RowCodecError(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_encode() -> Result<()> {
        let batch = vec![
            vec![Data::Bool(true), Data::Int32(12)],
            vec![Data::Bool(false), Data::Int32(11)],
        ];
        let row_batch = RowRecordBatch {
            batch,
            schema_version: 1,
        };
        let encoded: Vec<u8> = encode(&row_batch)?;
        let new_row_batch: RowRecordBatch = decode(&encoded[..])?;
        assert_eq!(row_batch.schema_version, new_row_batch.schema_version);
        assert_eq!(row_batch.batch.len(), new_row_batch.batch.len());
        Ok(())
    }
}
