//
//
// lib.rs
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
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DB3Error {
    #[error("invalid db3 address")]
    InvalidAddress,
    #[error("fail to load key pair {0}")]
    LoadKeyPairError(String),
    #[error("fail to sign a message with error {0}")]
    SignError(String),
    #[error("fail to verify the request with error {0}")]
    VerifyFailed(String),
    #[error("invalid signature siwith error {0}")]
    InvalidSignature(String),
    #[error("fail to codec key with error {0}")]
    KeyCodecError(String),
    #[error("fail to verify the owner with error {0}")]
    OwnerVerifyFailed(String),
    #[error("fail to apply mutation with error {0}")]
    ApplyMutationError(String),
    #[error("fail to submit mutation session with error {0}")]
    SubmitMutationError(String),
    #[error("fail to submit request with error {0}")]
    SubmitRequestError(String),
    #[error("fail to apply bill with error {0}")]
    ApplyBillError(String),
    #[error("fail to query bill with error {0}")]
    BillQueryError(String),
    #[error("fail to apply account with error {0}")]
    ApplyAccountError(String),
    #[error("fail to apply commit with error {0}")]
    ApplyCommitError(String),
    #[error("fail to apply database with error {0}")]
    ApplyDatabaseError(String),
    #[error("fail to apply document with error {0}")]
    ApplyDocumentError(String),
    #[error("fail to get commit with error {0}")]
    GetCommitError(String),
    #[error("fail to query account with error {0}")]
    GetAccountError(String),
    #[error("out of gas with error {0}")]
    OutOfGasError(String),
    #[error("fail to call bill sdk with error {0}")]
    BillSDKError(String),
    #[error("hash codec error")]
    HashCodecError,
    #[error("fail to query kv error {0}")]
    QueryKvError(String),
    #[error("fail to query, invalid session status {0}")]
    QuerySessionStatusError(String),
    #[error("fail to verify query session {0}")]
    QuerySessionVerifyError(String),
    #[error("fail to query database {0}")]
    QueryDatabaseError(String),

    #[error("the address does not match the public key")]
    InvalidSigner,
    #[error("fail to generate key for {0}")]
    SignatureKeyGenError(String),
    #[error("fail to sign message for {0}")]
    SignMessageError(String),
    #[error("fail to decode document for {0}")]
    DocumentDecodeError(String),
    #[error("fail to query document {0}")]
    QueryDocumentError(String),
    #[error("invalid op entry id bytes")]
    InvalidOpEntryIdBytes,
    #[error("invalid document id bytes")]
    InvalidDocumentIdBytes,
    #[error("invalid document bytes {0}")]
    InvalidDocumentBytes(String),
    #[error("invalid collection id bytes {0}")]
    InvalidCollectionIdBytes(String),
    #[error("invalid index id bytes {0}")]
    InvalidIndexIdBytes(String),
    #[error("document not exist with target id {0}")]
    DocumentNotExist(String),
    #[error("document modified permission error")]
    DocumentModifiedPermissionError,
    #[error("fail to store event for {0}")]
    StoreEventError(String),
    #[error("fail to store faucet for {0}")]
    StoreFaucetError(String),
    #[error("invalid filter value {0}")]
    InvalidFilterValue(String),
    #[error("invalid filter op {0}")]
    InvalidFilterOp(String),
    #[error("invalid filter type {0}")]
    InvalidFilterType(String),
    #[error("index not found for filed filter {0}")]
    IndexNotFoundForFiledFilter(String),
    #[error("invalid filter json string {0}")]
    InvalidFilterJson(String),
    #[error("invalid json string {0}")]
    InvalidJson(String),
    #[error("fail to request faucet for {0}")]
    RequestFaucetError(String),
    #[error("fail to fetch faucet for {0}")]
    FetchBlockError(String),
    #[error("fail to open db3 with path {0} for error {1}")]
    OpenStoreError(String, String),
    #[error("fail to write store for error {0}")]
    WriteStoreError(String),
    #[error("fail to read store for error {0}")]
    ReadStoreError(String),
    #[error("fail to rollup data for error {0}")]
    RollupError(String),
    #[error("fail to implement arware op for error {0}")]
    ArwareOpError(String),
    #[error("invalid collection name for error {0}")]
    InvalidCollectionNameError(String),
    #[error("invalid mutation for error {0}")]
    InvalidMutationError(String),
    #[error("invalid key path for error {0}")]
    InvalidKeyPathError(String),
    #[error("invalid arweave url for error {0}")]
    InvalidArUrlError(String),
    #[error("invalid database desc for error {0}")]
    InvalidDescError(String),

    #[error("database with addr {0} was not found")]
    DatabaseNotFound(String),
    #[error("database with addr {0} already exist")]
    DatabaseAlreadyExist(String),
    #[error("You have no permission to delete the database")]
    DatabasePermissionDenied(),
    #[error("collection with name {0} was not found in db {1}")]
    CollectionNotFound(String, String),
    #[error("collection {0} already exist in db {1}")]
    CollectionAlreadyExist(String, String),
    #[error("You have no permission to modify the collection")]
    CollectionPermissionDenied(),
}

pub type Result<T> = std::result::Result<T, DB3Error>;
