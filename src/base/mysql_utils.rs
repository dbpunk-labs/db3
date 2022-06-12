//
//
// mysql_utils.rs
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

use crate::codec::row_codec::{Data, RowRecordBatch};
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{RtStoreColumnDesc, RtStoreSchemaDesc, RtStoreType};
use arrow::array::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDateTime;
use msql_srv::Column as MySQLColumn;
use msql_srv::ColumnFlags;
use msql_srv::ColumnType;
use msql_srv::OkResponse;
use msql_srv::QueryResultWriter;
use sqlparser::ast::{ColumnDef, ColumnOption, DataType as SPDataType, Expr, Value};
uselog!(info, warn);

macro_rules! type_mapping {
    ($columns:ident, $right_type:ident, $field:ident) => {
        let col = MySQLColumn {
            table: "".to_string(),
            column: $field.name().to_string(),
            coltype: ColumnType::$right_type,
            colflags: ColumnFlags::empty(),
        };
        $columns.push(col);
    };
}

pub fn sql_to_row_batch(schema: &RtStoreSchemaDesc, values: &[Expr]) -> Result<RowRecordBatch> {
    let mut row: Vec<Data> = Vec::new();
    for (i, item) in values.iter().enumerate().take(schema.columns.len()) {
        let column_desc = &schema.columns[i];
        let ctype = RtStoreType::from_i32(column_desc.ctype);
        if let (Expr::Value(v), Some(local_type)) = (item, ctype) {
            let data = sql_value_to_data(v, &local_type)?;
            row.push(data);
        } else {
            warn!("invalid expr {}", item);
        }
    }
    Ok(RowRecordBatch {
        batch: vec![row],
        schema_version: 1,
    })
}

pub fn sql_value_to_data(val: &Value, store_type: &RtStoreType) -> Result<Data> {
    match (store_type, val) {
        (RtStoreType::KStringUtf8, Value::SingleQuotedString(s)) => {
            Ok(Data::Varchar(s.to_string()))
        }
        (RtStoreType::KStringUtf8, Value::DoubleQuotedString(s)) => {
            Ok(Data::Varchar(s.to_string()))
        }
        (RtStoreType::KBigInt, Value::Number(v, _)) => {
            let val_int: i64 = v.parse().unwrap();
            Ok(Data::Int64(val_int))
        }
        (RtStoreType::KInt, Value::Number(v, _)) => {
            let val_int: i32 = v.parse().unwrap();
            Ok(Data::Int32(val_int))
        }
        (RtStoreType::KFloat, Value::Number(v, _)) => {
            let val: f32 = v.parse().unwrap();
            Ok(Data::Float(val))
        }
        (RtStoreType::KDouble, Value::Number(v, _)) => {
            let val: f64 = v.parse().unwrap();
            Ok(Data::Double(val))
        }
        (RtStoreType::KTimestampMillsSecond, Value::SingleQuotedString(s))
        | (RtStoreType::KTimestampMillsSecond, Value::DoubleQuotedString(s)) => {
            let time = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S").unwrap();
            let ts = time.timestamp() * 1000;
            Ok(Data::Timestamp(ts as u64))
        }
        (_, _) => Err(RTStoreError::TableTypeMismatchError {
            left: "left".to_string(),
            right: "right".to_string(),
        }),
    }
}

pub fn record_batch_schema_to_mysql_schema(schema: &SchemaRef) -> Result<Vec<MySQLColumn>> {
    let mut mysql_cols = vec![];
    for field in schema.fields() {
        // all mysql types go to
        // https://github.com/blackbeam/rust_mysql_common/blob/master/src/constants.rs#L587
        // all parquet types go to
        // https://github.com/apache/arrow-rs/blob/master/arrow/src/datatypes/datatype.rs#L43
        match field.data_type() {
            DataType::Boolean => {
                type_mapping!(mysql_cols, MYSQL_TYPE_BIT, field);
            }
            DataType::Int8 => {
                type_mapping!(mysql_cols, MYSQL_TYPE_TINY, field);
            }
            DataType::Int16 => {
                type_mapping!(mysql_cols, MYSQL_TYPE_SHORT, field);
            }
            DataType::Int32 => {
                type_mapping!(mysql_cols, MYSQL_TYPE_LONG, field);
            }
            DataType::Int64 => {
                type_mapping!(mysql_cols, MYSQL_TYPE_LONGLONG, field);
            }
            DataType::Float32 => {
                type_mapping!(mysql_cols, MYSQL_TYPE_FLOAT, field);
            }
            DataType::Float64 => {
                type_mapping!(mysql_cols, MYSQL_TYPE_FLOAT, field);
            }
            DataType::Timestamp(..) => {
                type_mapping!(mysql_cols, MYSQL_TYPE_TIMESTAMP, field);
            }
            DataType::Utf8 => {
                type_mapping!(mysql_cols, MYSQL_TYPE_STRING, field);
            }
            DataType::Decimal(..) => {
                type_mapping!(mysql_cols, MYSQL_TYPE_DECIMAL, field);
            }
            _ => {
                return Err(RTStoreError::TableSchemaConvertError(0));
            }
        }
    }
    Ok(mysql_cols)
}

macro_rules! mysql_data_convert {
    ($row_idx:ident, $column_idx:ident, $data_type:ident,
     $writer:ident, $record_batch:ident) => {
        let arr = $record_batch
            .column($column_idx)
            .as_any()
            .downcast_ref::<$data_type>()
            .expect("Failed to downcast");
        $writer
            .write_col(arr.value($row_idx))
            .expect("fail to write col to writer");
    };
}

pub fn write_batch_to_resultset<'a, W: std::io::Write + Send>(
    record_batches: &[RecordBatch],
    results: QueryResultWriter<'a, W>,
) -> Result<()> {
    if record_batches.is_empty() {
        results.completed(OkResponse::default()).unwrap();
        return Ok(());
    }
    let schema = record_batches[0].schema();
    let mysql_schema = record_batch_schema_to_mysql_schema(&schema)?;
    let mut rw = results.start(&mysql_schema)?;
    for batch in record_batches {
        for i in 0..batch.num_rows() {
            for j in 0..batch.num_columns() {
                let data_type = schema.field(j).data_type();
                match data_type {
                    DataType::Int8 => {
                        mysql_data_convert!(i, j, Int8Array, rw, batch);
                    }
                    DataType::Int16 => {
                        mysql_data_convert!(i, j, Int16Array, rw, batch);
                    }
                    DataType::Int32 => {
                        mysql_data_convert!(i, j, Int32Array, rw, batch);
                    }
                    DataType::Int64 => {
                        mysql_data_convert!(i, j, Int64Array, rw, batch);
                    }
                    DataType::Float32 => {
                        mysql_data_convert!(i, j, Float32Array, rw, batch);
                    }
                    DataType::Float64 => {
                        mysql_data_convert!(i, j, Float64Array, rw, batch);
                    }
                    DataType::Utf8 => {
                        mysql_data_convert!(i, j, StringArray, rw, batch);
                    }
                    DataType::Timestamp(tu, _) => match tu {
                        TimeUnit::Second => {
                            let arr = batch
                                .column(j)
                                .as_any()
                                .downcast_ref::<TimestampSecondArray>()
                                .expect("Failed to downcast");
                            let v = arr.value_as_datetime(i);
                            rw.write_col(v)?;
                        }
                        TimeUnit::Millisecond => {
                            let arr = batch
                                .column(j)
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .expect("Failed to downcast");
                            let v = arr.value_as_datetime(i);
                            rw.write_col(v)?;
                        }
                        TimeUnit::Microsecond => {
                            let arr = batch
                                .column(j)
                                .as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .expect("Failed to downcast");
                            let v = arr.value_as_datetime(i);
                            rw.write_col(v)?;
                        }
                        TimeUnit::Nanosecond => {
                            let arr = batch
                                .column(j)
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .expect("Failed to downcast");
                            let v = arr.value_as_datetime(i);
                            rw.write_col(v)?;
                        }
                    },
                    _ => {
                        return Err(RTStoreError::TableSchemaConvertError(0));
                    }
                }
            }
            rw.end_row()?;
        }
    }
    rw.finish()?;
    Ok(())
}

pub fn sql_to_table_desc(columns: &Vec<ColumnDef>) -> Result<RtStoreSchemaDesc> {
    let mut rtstore_columns: Vec<RtStoreColumnDesc> = Vec::new();
    for column in columns {
        let rtstore_type = match column.data_type {
            SPDataType::TinyInt(_) => Ok(RtStoreType::KTinyInt),
            SPDataType::SmallInt(_) => Ok(RtStoreType::KSmallInt),
            SPDataType::Int(_) => Ok(RtStoreType::KInt),
            SPDataType::BigInt(_) => Ok(RtStoreType::KBigInt),
            SPDataType::Float(_) => Ok(RtStoreType::KFloat),
            SPDataType::Timestamp => Ok(RtStoreType::KTimestampMillsSecond),
            SPDataType::Varchar(_) | SPDataType::String => Ok(RtStoreType::KStringUtf8),
            SPDataType::Double => Ok(RtStoreType::KDouble),
            SPDataType::Decimal(..) => Ok(RtStoreType::KDecimal),
            _ => {
                warn!("{} is not supported currently", column);
                Err(RTStoreError::TableSchemaConvertError(0))
            }
        }?;
        let mut null_allowed = true;

        if column.options.len() > 0 && ColumnOption::NotNull == column.options[0].option {
            null_allowed = false;
        }
        let rtstore_column = RtStoreColumnDesc {
            name: column.name.value.to_string(),
            ctype: rtstore_type as i32,
            null_allowed,
        };
        rtstore_columns.push(rtstore_column);
    }
    Ok(RtStoreSchemaDesc {
        columns: rtstore_columns,
        version: 1,
    })
}
