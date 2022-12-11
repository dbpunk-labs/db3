//
// hash_util.rs
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

use db3_error::{DB3Error, Result};
use subtle_encoding::base64;
use tendermint::hash::Hash;

#[warn(dead_code)]
pub fn base64_to_hash(data: &str) -> Result<Hash> {
    let decoded = base64::decode(data).map_err(|_| DB3Error::HashCodecError)?;
    Hash::try_from(decoded).map_err(|_| DB3Error::HashCodecError)
}
