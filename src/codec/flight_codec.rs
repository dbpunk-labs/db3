//
//
// flight_codec.rs
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

use crate::proto::rtstore_compute_proto::{FlightData, FlightDescriptor};
use arrow::datatypes::Schema;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc::{
    convert, size_prefixed_root_as_message, writer, writer::EncodedData, writer::IpcWriteOptions,
};
/// SchemaAsIpc represents a pairing of a `Schema` with IpcWriteOptions
pub struct SchemaAsIpc<'a> {
    pub pair: (&'a Schema, &'a IpcWriteOptions),
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

// Implement a bunch of useful traits for various conversions, displays,
// etc...

// Deref

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

impl From<SchemaAsIpc<'_>> for SchemaResult {
    fn from(schema_ipc: SchemaAsIpc) -> Self {
        let IpcMessage(vals) = flight_schema_as_flatbuffer(schema_ipc.0, schema_ipc.1);
        SchemaResult { schema: vals }
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
