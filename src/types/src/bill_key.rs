//
// bill_key.rs
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
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use db3_error::{DB3Error, Result};
use ethereum_types::Address as AccountAddress;
//TODO add shard id
const BLOCK_BILL: &str = "BLOCK_BILL";

pub struct BillKey(pub u64, pub u64);
const BILL_KEY_SIZE: usize = BLOCK_BILL.len() + 16;
impl BillKey {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded_key = BLOCK_BILL.as_bytes().to_vec();
        encoded_key
            .write_u64::<BigEndian>(self.0)
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        encoded_key
            .write_u64::<BigEndian>(self.1)
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        Ok(encoded_key)
    }
    pub fn decode(data: &[u8]) -> Result<Self> {
        ensure_len_eq(data, BILL_KEY_SIZE)
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        let start_offset = BLOCK_BILL.len();
        let block_height = (&data[start_offset..])
            .read_u64::<BigEndian>()
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        let id = (&data[start_offset + 8..])
            .read_u64::<BigEndian>()
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        Ok(Self(block_height, id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_address_from_pk;
    use fastcrypto::secp256k1::Secp256k1PublicKey;
    use fastcrypto::traits::ToFromBytes;
    #[test]
    fn it_billkey_encode() -> Result<()> {
        let bk = BillKey(1, 10);
        let bk_encoded_key1 = bk.encode()?;
        let bk = BillKey(1, 11);
        let bk_encoded_key2 = bk.encode()?;
        assert!(bk_encoded_key2.cmp(&bk_encoded_key1) == std::cmp::Ordering::Greater);
        let bk = BillKey(1, 9);
        let bk_encoded_key3 = bk.encode()?;
        assert!(bk_encoded_key3.cmp(&bk_encoded_key2) == std::cmp::Ordering::Less);
        let bk_decoded = BillKey::decode(bk_encoded_key3.as_ref())?;
        assert_eq!(bk_decoded.0, bk.0);
        assert_eq!(bk_decoded.1, bk.1);
        Ok(())
    }
}
