//
//
// flight_codec.rs
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

use crate::error::{DB3Error, Result};
use crate::proto::db3_base_proto::FlightData;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc::{convert, reader, writer, writer::EncodedData, writer::IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::{convert::TryFrom, ops::Deref};
/// SchemaAsIpc represents a pairing of a `Schema` with IpcWriteOptions
pub struct SchemaAsIpc<'a> {
    pub pair: (&'a Schema, &'a IpcWriteOptions),
}

impl<'a> SchemaAsIpc<'a> {
    pub fn new(schema: &'a Schema, options: &'a IpcWriteOptions) -> Self {
        SchemaAsIpc {
            pair: (schema, options),
        }
    }
}

/// IpcMessage represents a `Schema` in the format expected in
/// `FlightInfo.schema`
#[derive(Debug)]
pub struct IpcMessage(pub Vec<u8>);

// Useful conversion functions
fn flight_schema_as_encoded_data(arrow_schema: &Schema, options: &IpcWriteOptions) -> EncodedData {
    let data_gen = writer::IpcDataGenerator::default();
    data_gen.schema_to_bytes(arrow_schema, options)
}

fn flight_schema_as_flatbuffer(schema: &Schema, options: &IpcWriteOptions) -> IpcMessage {
    let encoded_data = flight_schema_as_encoded_data(schema, options);
    IpcMessage(encoded_data.ipc_message)
}

impl Deref for IpcMessage {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> Deref for SchemaAsIpc<'a> {
    type Target = (&'a Schema, &'a IpcWriteOptions);
    fn deref(&self) -> &Self::Target {
        &self.pair
    }
}

impl From<EncodedData> for FlightData {
    fn from(data: EncodedData) -> Self {
        FlightData {
            data_header: data.ipc_message,
            data_body: data.arrow_data,
            ..Default::default()
        }
    }
}

impl From<SchemaAsIpc<'_>> for FlightData {
    fn from(schema_ipc: SchemaAsIpc) -> Self {
        let IpcMessage(vals) = flight_schema_as_flatbuffer(schema_ipc.0, schema_ipc.1);
        FlightData {
            data_header: vals,
            ..Default::default()
        }
    }
}

pub fn flight_data_from_arrow_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let data_gen = writer::IpcDataGenerator::default();
    let mut dictionary_tracker = writer::DictionaryTracker::new(false);
    let (encoded_dictionaries, encoded_batch) = data_gen
        .encoded_batch(batch, &mut dictionary_tracker, options)
        .expect("DictionaryTracker configured above to not error on replacement");

    let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
    let flight_batch = encoded_batch.into();
    (flight_dictionaries, flight_batch)
}

/// Convert `FlightData` (with supplied schema and dictionaries) to an arrow `RecordBatch`.
pub fn flight_data_to_arrow_batch(
    data: &FlightData,
    schema: SchemaRef,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
) -> Result<RecordBatch> {
    // check that the data_header is a record batch message
    let message = arrow::ipc::root_as_message(&data.data_header[..])
        .map_err(|err| DB3Error::RecordBatchCodecError(format!("fail to encode for {}", err)))?;
    message
        .header_as_record_batch()
        .ok_or_else(|| {
            DB3Error::RecordBatchCodecError(format!("fail to encode for {}", "codec header error"))
        })
        .map(|batch| {
            reader::read_record_batch(
                &data.data_body,
                batch,
                schema,
                dictionaries_by_id,
                None,
                &message.version(),
            )
            .map_err(|err| {
                DB3Error::RecordBatchCodecError(format!("fail to read record batch for {}", err))
            })
        })?
}

impl TryFrom<&FlightData> for Schema {
    type Error = ArrowError;
    fn try_from(data: &FlightData) -> ArrowResult<Self> {
        convert::schema_from_bytes(&data.data_header[..]).map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to convert flight data to Arrow schema: {}",
                err
            ))
        })
    }
}
