//
// key.rs
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
use ethereum_types::Address as AccountAddress;
const NAMESPACE: &str = "_NAMESPACE_";
const MAX_USE_KEY_LEN: usize = 32;
const MAX_NAMESPACE_LEN: usize = 16;
const MIN_KEY_TOTAL_LEN: usize = AccountAddress::len_bytes() + NAMESPACE.len();

/// account_address + NAMESPACE + ns  + user_key
pub struct Key<'a>(pub AccountAddress, pub &'a [u8], pub &'a [u8]);

impl<'a> Key<'a> {
    ///
    /// encode the key
    ///
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.1.len() > MAX_NAMESPACE_LEN || self.2.len() > MAX_USE_KEY_LEN {
            return Err(DB3Error::KeyCodecError(
                "the length of namespace or key exceeds the limit".to_string(),
            ));
        }
        let mut encoded_key = self.0.as_ref().to_vec();
        encoded_key.extend_from_slice(NAMESPACE.as_bytes());
        encoded_key.extend_from_slice(self.1);
        encoded_key.extend_from_slice(self.2);
        Ok(encoded_key)
    }

    ///
    /// decode the key
    ///
    pub fn decode(data: &'a [u8], ns: &'a [u8]) -> Result<Self> {
        if data.len() <= MIN_KEY_TOTAL_LEN {
            return Err(DB3Error::KeyCodecError(
                "the length of data is invalid".to_string(),
            ));
        }
        let key_start_offset = MIN_KEY_TOTAL_LEN + ns.len();
        let data_slice: &[u8; AccountAddress::len_bytes()] = &data[..AccountAddress::len_bytes()]
            .try_into()
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        let addr = AccountAddress::from(data_slice);
        Ok(Self(addr, ns, &data[key_start_offset..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_a_static_address;

    #[test]
    fn it_key_serde() {
        let addr = get_a_static_address();
        let ns: &str = "ns1";
        let k: &str = "k1";
        let key = Key(addr, ns.as_bytes(), k.as_bytes());
        let key_encoded = key.encode();
        assert!(key_encoded.is_ok());
        let key_decoded = Key::decode(key_encoded.as_ref().unwrap(), ns.as_bytes());
        assert!(key_decoded.is_ok());
        assert_eq!(key_decoded.unwrap().0, addr);
    }

    #[test]
    fn it_key_serde_cmp() -> Result<()> {
        let addr = get_a_static_address();
        let ns: &str = "ns1";
        let k: &str = "k1";
        let key = Key(addr, ns.as_bytes(), k.as_bytes());
        let key_encoded1 = key.encode()?;
        let ns: &str = "ns1";
        let k: &str = "k2";
        let key = Key(addr, ns.as_bytes(), k.as_bytes());
        let key_encoded2 = key.encode()?;
        assert!(key_encoded1.cmp(&key_encoded1) == std::cmp::Ordering::Equal);
        assert!(key_encoded1.cmp(&key_encoded2) == std::cmp::Ordering::Less);
        Ok(())
    }

    #[test]
    fn test_store_kv() {}
}
