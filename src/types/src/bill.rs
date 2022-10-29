//
// bill.rs
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
const BILL: &str = "BILL";

pub enum BillType {
    MutationBill {
        service_fee: u64,
        mutation_id: u64,
    },
    QueryBill {
        service_fee: u64,
        session_id: u64,
        service_addr: AccountAddress,
    },
}

pub struct Bill {
    /// the type of bill
    bill_type: BillType,
    block_heght: u64,
    gas_fee: u64,
    /// the time of generating the bill
    ctime: u64,
    bill_id: u64,
}

/// billkey = address + BILL + u64
pub struct BillKey(AccountAddress, u64);
const BILL_KEY_SIZE: usize = AccountAddress::len_bytes() + BILL.len() + 8;

impl BillKey {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded_key = self.0.as_ref().to_vec();
        encoded_key.extend_from_slice(BILL.as_bytes());
        encoded_key
            .write_u64::<BigEndian>(self.1)
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        Ok(encoded_key)
    }
    pub fn decode(data: &[u8]) -> Result<Self> {
        ensure_len_eq(data, BILL_KEY_SIZE)
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        let data_slice: &[u8; AccountAddress::len_bytes()] = &data[..AccountAddress::len_bytes()]
            .try_into()
            .expect("slice with incorrect length");
        let addr = AccountAddress::from(data_slice);
        let start_offset = AccountAddress::len_bytes() + BILL.len();
        let id = (&data[start_offset..])
            .read_u64::<BigEndian>()
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        Ok(Self(addr, id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_address_from_pk;
    use fastcrypto::secp256k1::Secp256k1PublicKey;
    use fastcrypto::traits::ToFromBytes;
    use hex;
    #[test]
    fn it_billkey_encode() -> Result<()> {
        let pk = Secp256k1PublicKey::from_bytes(
            &hex::decode("03ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138")
                .unwrap(),
        )
        .unwrap();
        let address = get_address_from_pk(&pk.pubkey);
        let bk = BillKey(address, 10);
        let bk_encoded_key1 = bk.encode()?;
        let address = get_address_from_pk(&pk.pubkey);
        let bk = BillKey(address, 11);
        let bk_encoded_key2 = bk.encode()?;
        assert!(bk_encoded_key2.cmp(&bk_encoded_key1) == std::cmp::Ordering::Greater);
        let address = get_address_from_pk(&pk.pubkey);
        let bk = BillKey(address, 9);
        let bk_encoded_key3 = bk.encode()?;
        assert!(bk_encoded_key3.cmp(&bk_encoded_key2) == std::cmp::Ordering::Less);
        let bk_decoded = BillKey::decode(bk_encoded_key3.as_ref())?;
        assert_eq!(bk_decoded.0, bk.0);
        assert_eq!(bk_decoded.1, bk.1);
        Ok(())
    }
}
