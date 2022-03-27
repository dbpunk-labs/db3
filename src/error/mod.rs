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

use std::fmt;

/// The error system for rtstore
pub enum RTStoreError {
    TableNotFoundError { tname: String },
    //
    FSInvalidFileError { path: String },
}

impl fmt::Display for RTStoreError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RTStoreError::TableNotFoundError { tname } => {
                write!(f, "table with name {} is not found", tname)
            }
            RTStoreError::FSInvalidFileError { path } => {
                write!(f, "bad file with name or path {}", path)
            }
        }
    }
}

/// The Result for rtstore
pub type Result<T> = std::result::Result<T, RTStoreError>;
