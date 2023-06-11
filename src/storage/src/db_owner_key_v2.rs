//
// db_owner_key_v2.rs
// Copyright (C) 2023 db3.network Author imotai <codego.me@gmail.com>
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

use db3_crypto::db3_address::DB3Address;
use db3_error::Result;

pub struct DbOwnerKey<'a>(pub &'a DB3Address, pub u64, pub u32);

impl<'a> DbOwnerKey<'a> {
    ///
    /// encode the database key
    ///
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded_key = self.0.as_ref().to_vec();
        encoded_key.extend_from_slice(self.1.to_be_bytes().as_ref());
        encoded_key.extend_from_slice(self.2.to_be_bytes().as_ref());
        Ok(encoded_key)
    }
}
