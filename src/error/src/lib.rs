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
    #[error("fail to require lock from state")]
    StateLockBusyError,
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
}

pub type Result<T> = std::result::Result<T, DB3Error>;
