//
// account_key.rs
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
use super::ensure_len_eq;
use db3_crypto::db3_address::{DB3Address, DB3_ADDRESS_LENGTH};
use db3_error::{DB3Error, Result};

const ACCOUNT_ID: &str = "_ACCOUNT_";

pub struct AccountKey(pub DB3Address);
const ACCOUNT_KEY_SIZE: usize = DB3_ADDRESS_LENGTH + ACCOUNT_ID.len();

impl AccountKey {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded_key = self.0.as_ref().to_vec();
        encoded_key.extend_from_slice(ACCOUNT_ID.as_bytes());
        Ok(encoded_key)
    }
    pub fn decode(data: &[u8]) -> Result<Self> {
        ensure_len_eq(data, ACCOUNT_KEY_SIZE)
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        let data_slice: &[u8; DB3_ADDRESS_LENGTH] = &data[..DB3_ADDRESS_LENGTH]
            .try_into()
            .expect("slice with incorrect length");
        let addr = DB3Address::from(data_slice);
        Ok(Self(addr))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
