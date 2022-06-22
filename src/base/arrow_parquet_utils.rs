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
    Array, ArrayRef, BooleanBuilder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    StringBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{
    DataType, Field as ArrowField, Schema, SchemaRef, TimeUnit, DECIMAL_MAX_PRECISION,
    DECIMAL_MAX_SCALE,
};

use datafusion::datasource::listing::PartitionedFile;

use arrow::record_batch::RecordBatch;
use datafusion::datafusion_data_access::{FileMeta, SizedFile};
use datafusion::scalar::ScalarValue;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use string_builder::Builder;
uselog!(info, debug, warn);

pub fn batches_to_paths(batches: &[RecordBatch]) -> Vec<PartitionedFile> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| PartitionedFile {
                file_meta: FileMeta {
                    last_modified: None,
                    sized_file: SizedFile {
                        path: "".to_string(),
                        size: batch
                            .columns()
                            .iter()
                            .map(|array| array.get_array_memory_size())
                            .sum::<usize>() as u64,
                    },
                },
                partition_values: (0..batch.columns().len())
                    .map(|col| ScalarValue::try_from_array(batch.column(col), row).unwrap())
                    .collect(),
                range: None,
            })
        })
        .collect()
}

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
    let mut writer = ArrowWriter::try_new(fd, schema.clone(), Some(properties))?;
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

impl RTStoreColumnBuilder {
    pub fn finish(&mut self) -> ArrayRef {
        match self {
            Self::RTStoreBooleanBuilder(b) => Arc::new(b.finish()),
            Self::RTStoreInt8Builder(b) => Arc::new(b.finish()),
            Self::RTStoreUInt8Builder(b) => Arc::new(b.finish()),
            Self::RTStoreInt16Builder(b) => Arc::new(b.finish()),
            Self::RTStoreUInt16Builder(b) => Arc::new(b.finish()),
            Self::RTStoreUInt16Builder(b) => Arc::new(b.finish()),
            Self::RTStoreInt32Builder(b) => Arc::new(b.finish()),
            Self::RTStoreUInt32Builder(b) => Arc::new(b.finish()),
            Self::RTStoreInt64Builder(b) => Arc::new(b.finish()),
            Self::RTStoreUInt64Builder(b) => Arc::new(b.finish()),
            Self::RTStoreStrBuilder(b) => Arc::new(b.finish()),
            Self::RTStoreTimestampNsBuilder(b) => Arc::new(b.finish()),
            Self::RTStoreTimestampMicrosBuilder(b) => Arc::new(b.finish()),
            Self::RTStoreTimestampMillsBuilder(b) => Arc::new(b.finish()),
        }
    }
}

macro_rules! primary_type_convert {
    ($left_builder:ident, $right_builder:ident, $data_type:ident,
     $builders:ident, $index:ident, $column:ident,
     $rows:ident) => {
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
    for rows in rows_batch.iter() {
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
                            rows
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
                            rows
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
                            rows
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
                            rows
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
                            rows
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
                            rows
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
                            rows
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
                            rows
                        );
                    }
                    DataType::Timestamp(_, _) => {
                        if builders.len() <= index {
                            let builder = RTStoreColumnBuilder::RTStoreTimestampMillsBuilder(
                                TimestampMillisecondBuilder::new(rows.batch.len()),
                            );
                            builders.push(builder);
                        }
                        let builder = &mut builders[index];
                        if let (
                            RTStoreColumnBuilder::RTStoreTimestampMillsBuilder(ts_builder),
                            Data::Timestamp(s),
                        ) = (builder, column)
                        {
                            ts_builder.append_value(*s as i64)?;
                        } else {
                            return Err(RTStoreError::TableTypeMismatchError {
                                left: "timestamp".to_string(),
                                right: column.name().to_string(),
                            });
                        }
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
    let mut array_refs: Vec<ArrayRef> = Vec::new();
    for mut builder in builders {
        array_refs.push(builder.finish());
    }
    let record_batch = RecordBatch::try_new(schema.clone(), array_refs)?;
    Ok(record_batch)
}

pub fn schema_to_recordbatch(schema: &SchemaRef) -> Result<RecordBatch> {
    let output_schema = Arc::new(Schema::new(vec![
        ArrowField::new("Field", DataType::Utf8, false),
        ArrowField::new("Type", DataType::Utf8, false),
        ArrowField::new("Null", DataType::Utf8, false),
        ArrowField::new("Key", DataType::Utf8, false),
        ArrowField::new("Default", DataType::Utf8, false),
        ArrowField::new("Extra", DataType::Utf8, false),
    ]));
    let mut rows: Vec<Vec<Data>> = Vec::new();
    for i in 0..schema.fields().len() {
        let mut row: Vec<Data> = Vec::new();
        let f = &schema.fields()[i];
        info!("{} field", f);
        row.push(Data::Varchar(f.name().clone()));
        match f.data_type() {
            DataType::Utf8 => {
                row.push(Data::Varchar("varchar(255)".to_string()));
            }
            DataType::Int8 => {
                row.push(Data::Varchar("tinyint".to_string()));
            }
            DataType::Int16 => {
                row.push(Data::Varchar("smallint".to_string()));
            }
            DataType::Int32 => {
                row.push(Data::Varchar("int".to_string()));
            }
            DataType::Int64 => {
                row.push(Data::Varchar("bigint".to_string()));
            }
            DataType::Float32 => {
                row.push(Data::Varchar("float".to_string()));
            }
            DataType::Float64 => {
                row.push(Data::Varchar("double".to_string()));
            }
            DataType::Timestamp(_, _) => {
                row.push(Data::Varchar("timestamp".to_string()));
            }
            _ => {
                row.push(Data::Varchar("unknow".to_string()));
            }
        }
        row.push(Data::Varchar("YES".to_string()));
        row.push(Data::Varchar("".to_string()));
        row.push(Data::Varchar("".to_string()));
        row.push(Data::Varchar("".to_string()));
        rows.push(row);
    }
    let rows = RowRecordBatch {
        batch: rows,
        schema_version: 0,
    };
    let data = LinkedList::<RowRecordBatch>::new();
    data.push_front(rows)?;
    rows_to_columns(&output_schema, &data)
}

pub fn schema_to_ddl_recordbatch(name: &str, schema: &SchemaRef) -> Result<RecordBatch> {
    let output_schema = Arc::new(Schema::new(vec![
        ArrowField::new("Table", DataType::Utf8, false),
        ArrowField::new("Create Table", DataType::Utf8, false),
    ]));
    let mut builder = Builder::default();
    builder.append(format!("create table `{}` (", name));
    for i in 0..schema.fields().len() {
        let f = &schema.fields()[i];
        if i > 0 {
            builder.append(",");
        }
        match f.data_type() {
            DataType::Int8 => {
                builder.append(format!("{} tinyint", f.name()));
            }
            DataType::Int16 => {
                builder.append(format!("{} smallint", f.name()));
            }
            DataType::Int32 => {
                builder.append(format!("{} int", f.name()));
            }
            DataType::Int64 => {
                builder.append(format!("{} bigint", f.name()));
            }
            DataType::Float32 => {
                builder.append(format!("{} float", f.name()));
            }
            DataType::Float64 => {
                builder.append(format!("{} double", f.name()));
            }
            DataType::Utf8 => {
                builder.append(format!("{} varchar(255)", f.name()));
            }
            DataType::Timestamp(_, _) => {
                builder.append(format!("{} timestamp", f.name()));
            }
            _ => {
                warn!("{:?} is unsupported", f);
            }
        }
    }
    builder.append(")");
    let ddl = builder.string().unwrap();
    let row = vec![Data::Varchar(name.to_string()), Data::Varchar(ddl)];
    let rows = RowRecordBatch {
        batch: vec![row],
        schema_version: 0,
    };
    let data = LinkedList::<RowRecordBatch>::new();
    data.push_front(rows)?;
    rows_to_columns(&output_schema, &data)
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
