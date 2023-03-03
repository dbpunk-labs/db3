//
// db_key.rs
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

use db3_crypto::id::{DbId, DBID_LENGTH};
use db3_error::{DB3Error, Result};

/// /db/{db_address}
pub struct DbKey(pub DbId);

const DATABASE: &str = "/db/";

impl DbKey {
    ///
    /// encode the database key
    ///
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded_key = DATABASE.as_bytes().to_vec();
        encoded_key.extend_from_slice(self.0.as_ref());
        Ok(encoded_key)
    }

    ///
    /// decode the database key
    ///
    #[allow(dead_code)]
    pub fn decode(data: &[u8]) -> Result<Self> {
        const MIN_KEY_TOTAL_LEN: usize = DBID_LENGTH + DATABASE.len();
        if data.len() < MIN_KEY_TOTAL_LEN {
            return Err(DB3Error::KeyCodecError(
                "the length of data is invalid".to_string(),
            ));
        }
        let address_offset = DATABASE.len();
        let data_slice: &[u8; DBID_LENGTH] = &data[address_offset..address_offset + DbId::length()]
            .try_into()
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        let id = DbId::from(data_slice);
        Ok(Self(id))
    }

    #[allow(dead_code)]
    #[inline]
    pub fn max() -> Self {
        DbKey(DbId::max_id())
    }

    #[allow(dead_code)]
    #[inline]
    pub fn min() -> Self {
        DbKey(DbId::min_id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_crypto::key_derive;
    use db3_crypto::{db3_address::DB3Address, signature_scheme::SignatureScheme};

    fn gen_address() -> DB3Address {
        let seed: [u8; 32] = [0; 32];
        let (address, _) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::ED25519).unwrap();
        address
    }

    #[test]
    fn it_key_serde() {
        let addr = gen_address();
        let id = DbId::from(addr);
        let key = DbKey(id);
        let key_encoded = key.encode();
        assert!(key_encoded.is_ok());
        let key_decoded = DbKey::decode(key_encoded.as_ref().unwrap());
        assert!(key_decoded.is_ok());
        let key2 = key_decoded.unwrap();
        assert!(key2.0 == id);
    }

    #[test]
    fn it_cmp() -> Result<()> {
        let min = DbKey::min().encode()?;
        let max = DbKey::max().encode()?;
        assert!(min.cmp(&min) == std::cmp::Ordering::Equal);
        assert!(min.cmp(&max) == std::cmp::Ordering::Less);
        Ok(())
    }
}
