//
//
// arrow_parquet_utils.rs
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
use crate::proto::rtstore_base_proto::{RtStoreSchemaDesc, RtStoreType};
use arrow::datatypes::{
    DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit, DECIMAL_MAX_PRECISION,
    DECIMAL_MAX_SCALE,
};
use std::sync::Arc;

pub fn table_desc_to_arrow_schema(desc: &RtStoreSchemaDesc) -> Result<SchemaRef> {
    let mut fields: Vec<ArrowField> = Vec::new();
    for column in &desc.columns {
        let dt = match RtStoreType::from_i32(column.ctype) {
            Some(t) => match t {
                RtStoreType::KBool => Ok(DataType::Boolean),
                RtStoreType::KSmallInt => Ok(DataType::Int16),
                RtStoreType::KInt => Ok(DataType::Int32),
                RtStoreType::KBigInt => Ok(DataType::Int64),
                RtStoreType::KFloat => Ok(DataType::Float32),
                RtStoreType::KDouble => Ok(DataType::Float64),
                RtStoreType::KDate => Ok(DataType::Date32),
                RtStoreType::KDecimal => {
                    Ok(DataType::Decimal(DECIMAL_MAX_PRECISION, DECIMAL_MAX_SCALE))
                }
                RtStoreType::KTimestampSecond => Ok(DataType::Timestamp(TimeUnit::Second, None)),
                RtStoreType::KTimestampMillsSecond => {
                    Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
                }
                RtStoreType::KTimestampMicroSecond => {
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                RtStoreType::KStringUtf8 => Ok(DataType::Utf8),
            },
            _ => Err(RTStoreError::TableSchemaConvertError(column.ctype)),
        }?;
        fields.push(ArrowField::new(&column.name, dt, column.null_allowed));
    }
    Ok(Arc::new(Schema::new(fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{RTStoreError, Result};
    use crate::proto::rtstore_base_proto::RtStoreColumnDesc;
    #[test]
    fn it_convert_desc_to_arrow_schema() -> Result<()> {
        let columns = vec![RtStoreColumnDesc {
            name: "col1".to_string(),
            ctype: RtStoreType::KBool as i32,
            null_allowed: true,
        }];
        let schema = RtStoreSchemaDesc {
            columns,
            version: 1,
        };
        let schema_ref = table_desc_to_arrow_schema(&schema)?;
        assert_eq!(1, schema_ref.fields().len());
        assert_eq!(&DataType::Boolean, schema_ref.fields()[0].data_type());
        assert_eq!("col1", schema_ref.fields()[0].name());
        assert!(schema_ref.fields()[0].is_nullable());
        Ok(())
    }
}
