//
// collection_key.rs
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
use db3_crypto::db3_address::{DB3Address, DB3_ADDRESS_LENGTH};
use db3_error::{DB3Error, Result};
use std::fmt;
const DOC: &str = "/doc";
/// DocOwnerKey with owner address, block id, order id, entry id
pub struct DocOwnerKeyV2(pub DB3Address, pub u64, pub u32, pub u32);

impl DocOwnerKeyV2 {
    ///
    /// encode the document owner key
    ///
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded_key = DOC.as_bytes().to_vec();
        encoded_key.extend_from_slice(self.0.as_ref());
        encoded_key.extend_from_slice(self.1.to_be_bytes().as_ref());
        encoded_key.extend_from_slice(self.2.to_be_bytes().as_ref());
        encoded_key.extend_from_slice(self.3.to_be_bytes().as_ref());
        encoded_key
    }

    ///
    /// decode the document owner key
    ///
    #[allow(dead_code)]
    pub fn decode(data: &[u8]) -> Result<Self> {
        const MIN_KEY_TOTAL_LEN: usize = DB3_ADDRESS_LENGTH + DOC.len();
        if data.len() < MIN_KEY_TOTAL_LEN {
            return Err(DB3Error::KeyCodecError(
                "the length of data is invalid".to_string(),
            ));
        }
        let address_offset = DOC.len();
        let data_slice: &[u8; DB3_ADDRESS_LENGTH] = &data
            [address_offset..address_offset + DB3_ADDRESS_LENGTH]
            .try_into()
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        let owner = DB3Address::from(data_slice);
        let block_id_offset = address_offset + DB3_ADDRESS_LENGTH;
        let block_id_slice: &[u8; 8] = &data[block_id_offset..block_id_offset + 8]
            .try_into()
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        let block_id = u64::from_be_bytes(*block_id_slice);

        let order_id_offset = block_id_offset + 8;
        let order_id_slice: &[u8; 4] = &data[order_id_offset..order_id_offset + 4]
            .try_into()
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        let order_id = u32::from_be_bytes(*order_id_slice);

        let entry_id_offset = order_id_offset + 4;
        let entry_id_slice: &[u8; 4] = &data[entry_id_offset..entry_id_offset + 4]
            .try_into()
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        let entry_id = u32::from_be_bytes(*entry_id_slice);
        Ok(Self(owner, block_id, order_id, entry_id))
    }
    /// decode the document key from string
    pub fn from_str(doc_key_str: &str) -> Result<Self> {
        let tokens: Vec<_> = doc_key_str.split("/").collect();
        let (_, prefix, owner_hex, block_id_str, order_id_str, entry_id_str) = (
            tokens[0], tokens[1], tokens[2], tokens[3], tokens[4], tokens[5],
        );
        if prefix != "doc" {
            return Err(DB3Error::KeyCodecError(
                "the prefix of key is invalid".to_string(),
            ));
        }
        let owner = DB3Address::from_hex(owner_hex)?;
        let block_id: u64 = block_id_str
            .parse()
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        let order_id: u32 = order_id_str
            .parse()
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        let entry_id: u32 = entry_id_str
            .parse()
            .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
        Ok(Self(owner, block_id, order_id, entry_id))
    }

    pub fn verify_owner(&self, owner: &DB3Address) -> Result<()> {
        if owner.as_ref() == self.0.as_ref() {
            Ok(())
        } else {
            Err(DB3Error::OwnerVerifyFailed(
                "the owner is invalid".to_string(),
            ))
        }
    }
}

impl fmt::Display for DocOwnerKeyV2 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}/{}",
            DOC,
            self.0.to_hex(),
            self.1,
            self.2,
            self.3
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {}

    #[test]
    fn encode_and_decode_ut() {
        let key = DocOwnerKeyV2(DB3Address::default(), 1, 2, 3);
        let encoded = key.encode();
        let decoded = DocOwnerKeyV2::decode(&encoded).unwrap();
        assert_eq!(key.0.to_hex(), decoded.0.to_hex());
        assert_eq!(key.1, decoded.1);
        assert_eq!(key.2, decoded.2);
        assert_eq!(key.3, decoded.3);
    }

    #[test]
    fn verify_owner_ut() {
        let key = DocOwnerKeyV2(DB3Address::default(), 1, 2, 3);
        let owner = DB3Address::default();
        let result = key.verify_owner(&owner);
        assert!(result.is_ok());
    }

    #[test]
    fn from_str_ut() {
        let key = DocOwnerKeyV2(DB3Address::default(), 1, 2, 3);
        let key_str = key.to_string();
        assert_eq!(
            "/doc/0x0000000000000000000000000000000000000000/1/2/3",
            key_str
        );
        let decoded = DocOwnerKeyV2::from_str(&key_str).unwrap();
        assert_eq!(key.0.to_hex(), decoded.0.to_hex());
        assert_eq!(key.1, decoded.1);
        assert_eq!(key.2, decoded.2);
        assert_eq!(key.3, decoded.3);
    }
}
