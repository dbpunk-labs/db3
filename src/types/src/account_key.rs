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

const ACCOUNT_ID: &str = "/ac/";

pub struct AccountKey<'a>(pub &'a DB3Address);
const ACCOUNT_KEY_SIZE: usize = DB3_ADDRESS_LENGTH + ACCOUNT_ID.len();

impl<'a> AccountKey<'a> {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded_key = ACCOUNT_ID.as_bytes().to_vec();
        encoded_key.extend_from_slice(self.0.as_ref());
        Ok(encoded_key)
    }

    pub fn decode(data: &[u8]) -> Result<DB3Address> {
        ensure_len_eq(data, ACCOUNT_KEY_SIZE)
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        let data_slice: &[u8; DB3_ADDRESS_LENGTH] = &data[ACCOUNT_ID.len()..]
            .try_into()
            .expect("slice with incorrect length");
        let addr = DB3Address::from(data_slice);
        Ok(addr)
    }
}

#[cfg(test)]
mod tests {
    use crate::account_key::AccountKey;
    use db3_crypto::key_derive;
    use db3_crypto::{db3_address::DB3Address, signature_scheme::SignatureScheme};
    fn gen_address() -> DB3Address {
        let seed: [u8; 32] = [0; 32];
        let (address, _) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::ED25519).unwrap();
        address
    }

    #[test]
    fn account_key_test() {
        let addr = gen_address();
        let key = AccountKey(&addr);
        let encoded_key = key.encode().unwrap();
        let addr2 = AccountKey::decode(encoded_key.as_ref()).unwrap();
        assert!(addr == addr2);
    }
}
