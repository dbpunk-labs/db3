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
    #[error("fail to sign a message with error {0}")]
    SignError(String),
    #[error("fail to verify the request with error {0}")]
    VerifyFailed(String),
    #[error("fail to codec key with error {0}")]
    KeyCodecError(String),
    #[error("fail to apply mutation with error {0}")]
    ApplyMutationError(String),
    #[error("fail to apply bill with error {0}")]
    ApplyBillError(String),
}

pub type Result<T> = std::result::Result<T, DB3Error>;
