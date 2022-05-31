//
//
// mod.rs
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

use arrow::error::ArrowError;
use parquet::errors::ParquetError;
use s3::error::S3Error;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::TokenizerError;
use std::io::{Error as IoError, ErrorKind};
use thiserror::Error;
use tonic::Status;

/// The error system for rtstore
#[derive(Debug, Error)]
pub enum RTStoreError {
    #[error("table with name {tname} was not found")]
    TableNotFoundError { tname: String },
    #[error("invalid table names for {error}")]
    TableInvalidNamesError { error: String },
    #[error("table with name {name} exists")]
    TableNamesExistError { name: String },
    #[error("table type mismatch left {left} and right {right}")]
    TableTypeMismatchError { left: String, right: String },
    #[error("table to arrow for error : {0}")]
    TableArrowError(ArrowError),
    #[error("table {table_id} encounter encoding or decoding error {err}")]
    TableCodecError { table_id: String, err: String },
    #[error("file with {path} is invalid")]
    FSInvalidFileError { path: String },
    #[error("filesystem io error:{0}")]
    FSIoError(IoError),
    #[error("reach the end of file")]
    FSIoEofError,
    #[error("fail to read log for {0}")]
    FSLogReaderError(String),
    #[error("parquet error: {0}")]
    FSParquetError(ParquetError),
    #[error("fail to convert {0} to rtstore column type")]
    TableSchemaConvertError(i32),
    #[error("the schema for table {name} is invalid, please check the input")]
    TableSchemaInvalidError { name: String },
    #[error("create table error for {err}")]
    MetaRpcCreateTableError { err: String },
    #[error("the {name} of cell store config is invalid for {err}")]
    CellStoreInvalidConfigError { name: String, err: String },
    #[error("the cell exist in memory node with tid {tid} and pid {pid}")]
    CellStoreExistError { tid: String, pid: i32 },
    #[error("the cell has not been found in memory node with tid {tid} and pid {pid}")]
    CellStoreNotFoundError { tid: String, pid: i32 },
    #[error("aws-s3: {0}")]
    CellStoreS3Error(S3Error),
    #[error("row codec error : {0}")]
    RowCodecError(bincode::Error),
    #[error("system busy for error : {0}")]
    BaseBusyError(String),
    #[error("memory node with endpoint {0} exists")]
    MemoryNodeExistError(String),
    #[error("not enough memory node")]
    MemoryNodeNotEnoughError,
    #[error("fail to connect to {0}")]
    NodeRPCError(String),
    #[error("invalid endpoint for node {name}")]
    NodeRPCInvalidEndpointError { name: String },
    #[error("fail to decode data from etcd for err {0}")]
    EtcdCodecError(String),
    #[error("meta store type mismatch")]
    MetaStoreTypeMisatchErr,
    #[error("the {name} for {key} has exist")]
    MetaStoreExistErr { name: String, key: String },
    #[error("encounter some etcd error {0}")]
    MetaStoreEtcdErr(etcd_client::Error),
    #[error("no meta store found")]
    MetaStoreNotFoundErr,
    #[error("fail to parse sql for error {0}")]
    SQLParseError(String),
}

/// convert io error to rtstore error
impl From<IoError> for RTStoreError {
    fn from(error: IoError) -> Self {
        RTStoreError::FSIoError(error)
    }
}

impl From<ParquetError> for RTStoreError {
    fn from(error: ParquetError) -> Self {
        RTStoreError::FSParquetError(error)
    }
}

impl From<ParserError> for RTStoreError {
    fn from(error: ParserError) -> Self {
        match error {
            ParserError::TokenizerError(e) => RTStoreError::SQLParseError(e),
            ParserError::ParserError(e) => RTStoreError::SQLParseError(e),
        }
    }
}

impl From<TokenizerError> for RTStoreError {
    fn from(err: TokenizerError) -> Self {
        RTStoreError::SQLParseError(err.message)
    }
}

impl From<S3Error> for RTStoreError {
    fn from(error: S3Error) -> Self {
        RTStoreError::CellStoreS3Error(error)
    }
}

impl From<ArrowError> for RTStoreError {
    fn from(error: ArrowError) -> Self {
        RTStoreError::TableArrowError(error)
    }
}

impl From<etcd_client::Error> for RTStoreError {
    fn from(error: etcd_client::Error) -> Self {
        RTStoreError::MetaStoreEtcdErr(error)
    }
}

impl From<RTStoreError> for IoError {
    fn from(error: RTStoreError) -> Self {
        match error {
            RTStoreError::FSIoError(e) => e,
            _ => IoError::from(ErrorKind::Other),
        }
    }
}

impl From<RTStoreError> for String {
    fn from(error: RTStoreError) -> Self {
        format!("{}", error)
    }
}

impl From<RTStoreError> for Status {
    fn from(error: RTStoreError) -> Self {
        match error {
            RTStoreError::TableInvalidNamesError { .. }
            | RTStoreError::TableSchemaConvertError { .. }
            | RTStoreError::TableSchemaInvalidError { .. }
            | RTStoreError::MetaRpcCreateTableError { .. } => Status::invalid_argument(error),
            RTStoreError::TableNotFoundError { .. }
            | RTStoreError::CellStoreNotFoundError { .. } => Status::not_found(error),
            RTStoreError::TableNamesExistError { .. }
            | RTStoreError::CellStoreExistError { .. } => Status::already_exists(error),
            _ => Status::internal(error),
        }
    }
}

/// The Result for rtstore
pub type Result<T> = std::result::Result<T, RTStoreError>;
