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

use crate::base::linked_list::LinkedList;
use crate::codec::row_codec::{Data, RowRecordBatch};
use crate::error::{RTStoreError, Result};
use crate::proto::rtstore_base_proto::{RtStoreSchemaDesc, RtStoreType};
use arrow::array::{
    ArrayRef, BooleanBuilder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{
    DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit, DECIMAL_MAX_PRECISION,
    DECIMAL_MAX_SCALE,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
uselog!(info, debug);

pub fn table_desc_to_arrow_schema(desc: &RtStoreSchemaDesc) -> Result<SchemaRef> {
    let mut fields: Vec<ArrowField> = Vec::new();
    for column in &desc.columns {
        let dt = match RtStoreType::from_i32(column.ctype) {
            Some(t) => match t {
                RtStoreType::KBool => Ok(DataType::Boolean),
                RtStoreType::KTinyInt => Ok(DataType::Int8),
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

pub fn dump_recordbatch(
    path: &Path,
    batches: &LinkedList<RecordBatch>,
    schema: &SchemaRef,
) -> Result<()> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::GZIP)
        .build();
    let fd = File::create(path)?;
    let mut writer = ArrowWriter::try_new(fd, schema.clone(), Some(properties.clone()))?;
    for batch in batches.iter() {
        writer.write(batch)?;
    }
    writer.close()?;
    Ok(())
}

#[allow(clippy::all)]
enum RTStoreColumnBuilder {
    RTStoreBooleanBuilder(BooleanBuilder),
    RTStoreInt8Builder(Int8Builder),
    RTStoreUInt8Builder(UInt8Builder),
    RTStoreInt16Builder(Int16Builder),
    RTStoreUInt16Builder(UInt16Builder),
    RTStoreInt32Builder(Int32Builder),
    RTStoreUInt32Builder(UInt32Builder),
    RTStoreInt64Builder(Int64Builder),
    RTStoreUInt64Builder(UInt64Builder),
    RTStoreStrBuilder(StringBuilder),
    RTStoreTimestampNsBuilder(TimestampNanosecondBuilder),
    RTStoreTimestampMicrosBuilder(TimestampMicrosecondBuilder),
    RTStoreTimestampMillsBuilder(TimestampMillisecondBuilder),
}

macro_rules! primary_type_convert {
    ($left_builder:ident, $right_builder:ident, $data_type:ident,
     $builders:ident, $index:ident, $column:ident,
     $rows:ident, $array_refs:ident, $c_index:ident,
     $e_index:ident) => {
        let bsize = $builders.len();
        if bsize <= $index {
            let builder =
                RTStoreColumnBuilder::$left_builder($right_builder::new($rows.batch.len()));
            $builders.push(builder);
        }
        let builder = &mut $builders[$index];
        if let (
            RTStoreColumnBuilder::$left_builder(internal_builder),
            Data::$data_type(internal_v),
        ) = (builder, $column)
        {
            internal_builder.append_value(*internal_v)?;
            if $c_index == $e_index && $array_refs.len() < bsize {
                $array_refs.push(Arc::new(internal_builder.finish()));
            }
        } else {
            return Err(RTStoreError::TableTypeMismatchError {
                left: "$data_type".to_string(),
                right: $column.name().to_string(),
            });
        }
    };
}

pub fn rows_to_columns(
    schema: &SchemaRef,
    rows_batch: &LinkedList<RowRecordBatch>,
) -> Result<RecordBatch> {
    if rows_batch.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }
    let mut builders: Vec<RTStoreColumnBuilder> = Vec::new();
    let mut array_refs: Vec<ArrayRef> = Vec::new();
    let end_index = rows_batch.size();
    let mut current_index = 0;
    for rows in rows_batch.iter() {
        current_index += 1;
        for r_index in 0..rows.batch.len() {
            let r = &rows.batch[r_index];
            for index in 0..schema.fields().len() {
                let field = &schema.fields()[index];
                let column = &r[index];
                match field.data_type() {
                    DataType::Boolean => {
                        primary_type_convert!(
                            RTStoreBooleanBuilder,
                            BooleanBuilder,
                            Bool,
                            builders,
                            index,
                            column,
                            rows,
                            array_refs,
                            current_index,
                            end_index
                        );
                    }
                    DataType::UInt8 => {
                        primary_type_convert!(
                            RTStoreUInt8Builder,
                            UInt8Builder,
                            UInt8,
                            builders,
                            index,
                            column,
                            rows,
                            array_refs,
                            current_index,
                            end_index
                        );
                    }
                    DataType::Int8 => {
                        primary_type_convert!(
                            RTStoreInt8Builder,
                            Int8Builder,
                            Int8,
                            builders,
                            index,
                            column,
                            rows,
                            array_refs,
                            current_index,
                            end_index
                        );
                    }
                    DataType::Int16 => {
                        primary_type_convert!(
                            RTStoreInt16Builder,
                            Int16Builder,
                            Int16,
                            builders,
                            index,
                            column,
                            rows,
                            array_refs,
                            current_index,
                            end_index
                        );
                    }
                    DataType::UInt16 => {
                        primary_type_convert!(
                            RTStoreUInt16Builder,
                            UInt16Builder,
                            UInt16,
                            builders,
                            index,
                            column,
                            rows,
                            array_refs,
                            current_index,
                            end_index
                        );
                    }
                    DataType::Int32 => {
                        primary_type_convert!(
                            RTStoreInt32Builder,
                            Int32Builder,
                            Int32,
                            builders,
                            index,
                            column,
                            rows,
                            array_refs,
                            current_index,
                            end_index
                        );
                    }
                    DataType::Int64 => {
                        primary_type_convert!(
                            RTStoreInt64Builder,
                            Int64Builder,
                            Int64,
                            builders,
                            index,
                            column,
                            rows,
                            array_refs,
                            current_index,
                            end_index
                        );
                    }
                    DataType::UInt64 => {
                        primary_type_convert!(
                            RTStoreUInt64Builder,
                            UInt64Builder,
                            UInt64,
                            builders,
                            index,
                            column,
                            rows,
                            array_refs,
                            current_index,
                            end_index
                        );
                    }
                    DataType::Utf8 => {
                        if builders.len() <= index {
                            let builder = RTStoreColumnBuilder::RTStoreStrBuilder(
                                StringBuilder::new(rows.batch.len()),
                            );
                            builders.push(builder);
                        }
                        let builder = &mut builders[index];
                        if let (
                            RTStoreColumnBuilder::RTStoreStrBuilder(str_builder),
                            Data::Varchar(s),
                        ) = (builder, column)
                        {
                            str_builder.append_value(s)?;
                            if r_index == rows.batch.len() - 1 {
                                array_refs.push(Arc::new(str_builder.finish()));
                            }
                        } else {
                            return Err(RTStoreError::TableTypeMismatchError {
                                left: "utf8".to_string(),
                                right: column.name().to_string(),
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    let record_batch = RecordBatch::try_new(schema.clone(), array_refs)?;
    Ok(record_batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::proto::rtstore_base_proto::RtStoreColumnDesc;
    use arrow::array::{
        Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array, UInt64Array, UInt8Array,
    };

    macro_rules! test_schema_convert {
        ($func:ident, $type:ident, $target_type:ident) => {
            #[test]
            fn $func() -> Result<()> {
                let columns = vec![RtStoreColumnDesc {
                    name: "col1".to_string(),
                    ctype: RtStoreType::$type as i32,
                    null_allowed: true,
                }];
                let schema = RtStoreSchemaDesc {
                    columns,
                    version: 1,
                };
                let schema_ref = table_desc_to_arrow_schema(&schema)?;
                assert_eq!(1, schema_ref.fields().len());
                assert_eq!(&DataType::$target_type, schema_ref.fields()[0].data_type());
                assert!(schema_ref.fields()[0].is_nullable());
                Ok(())
            }
        };
    }
    test_schema_convert!(test_type_convert_bool, KBool, Boolean);
    test_schema_convert!(test_type_convert_tiny_int, KTinyInt, Int8);
    test_schema_convert!(test_type_convert_small_int, KSmallInt, Int16);
    test_schema_convert!(test_type_convert_int, KInt, Int32);
    test_schema_convert!(test_type_convert_bigint, KBigInt, Int64);
    test_schema_convert!(test_type_convert_float, KFloat, Float32);
    test_schema_convert!(test_type_convert_double, KDouble, Float64);
    test_schema_convert!(test_type_convert_string, KStringUtf8, Utf8);
    test_schema_convert!(test_type_convert_date, KDate, Date32);

    #[test]
    fn test_schema_convert_complexe() -> Result<()> {
        let columns = vec![
            RtStoreColumnDesc {
                name: "col1".to_string(),
                ctype: RtStoreType::KDecimal as i32,
                null_allowed: true,
            },
            RtStoreColumnDesc {
                name: "col2".to_string(),
                ctype: RtStoreType::KTimestampSecond as i32,
                null_allowed: true,
            },
            RtStoreColumnDesc {
                name: "col3".to_string(),
                ctype: RtStoreType::KTimestampMillsSecond as i32,
                null_allowed: true,
            },
            RtStoreColumnDesc {
                name: "col4".to_string(),
                ctype: RtStoreType::KTimestampMicroSecond as i32,
                null_allowed: true,
            },
        ];
        let schema = RtStoreSchemaDesc {
            columns,
            version: 1,
        };
        let schema_ref = table_desc_to_arrow_schema(&schema)?;
        assert_eq!(4, schema_ref.fields().len());
        match (
            schema_ref.fields()[0].data_type(),
            schema_ref.fields()[1].data_type(),
            schema_ref.fields()[2].data_type(),
            schema_ref.fields()[3].data_type(),
        ) {
            (
                DataType::Decimal(_, _),
                DataType::Timestamp(tu1, _),
                DataType::Timestamp(tu2, _),
                DataType::Timestamp(tu3, _),
            ) => {
                assert_eq!(tu1, &TimeUnit::Second);
                assert_eq!(tu2, &TimeUnit::Millisecond);
                assert_eq!(tu3, &TimeUnit::Microsecond);
            }
            _ => {
                panic!("should not be here");
            }
        }
        Ok(())
    }
    macro_rules! test_num_convert {
        ($func:ident, $type:ident, $sys_type:tt, $builder:ident) => {
            #[test]
            fn $func() -> Result<()> {
                let fields = vec![ArrowField::new("col1", DataType::$type, false)];
                let schema = Arc::new(Schema::new(fields));
                let batch = vec![
                    vec![Data::$type(12 as $sys_type)],
                    vec![Data::$type(11 as $sys_type)],
                ];
                let row_batch = RowRecordBatch {
                    batch,
                    schema_version: 1,
                    id: "eth.price".to_string(),
                };
                let ll: LinkedList<RowRecordBatch> = LinkedList::new();
                ll.push_front(row_batch)?;
                let record_batch = rows_to_columns(&schema, &ll)?;
                let array = record_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<$builder>()
                    .expect("fail to down cast");
                assert_eq!(12 as $sys_type, array.value(0));
                assert_eq!(11 as $sys_type, array.value(1));
                Ok(())
            }
        };
    }
    test_num_convert!(test_int32_convert, Int32, i32, Int32Array);
    test_num_convert!(test_int8_convert, Int8, i8, Int8Array);
    test_num_convert!(test_uint8_convert, UInt8, u8, UInt8Array);
    test_num_convert!(test_int16_convert, Int16, i16, Int16Array);
    test_num_convert!(test_uint16_convert, UInt16, u16, UInt16Array);
    test_num_convert!(test_int64_convert, Int64, i64, Int64Array);
    test_num_convert!(test_uint64_convert, UInt64, u64, UInt64Array);
}
