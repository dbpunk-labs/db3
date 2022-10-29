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

use ethereum_types::Address as AccountAddress;
use db3_error::Result;
use byteorder::{BigEndian, WriteBytesExt};
use super::ensure_len_eq;


const BILL:&str = "BILL";
#[derive(
  Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize
)]
pub enum BillType {
    MutationBill {
        service_fee: u64,
        mutation_id: u64,
    }
    QueryBill {
        service_fee: u64,
        session_id: u64,
        service_addr: AccountAddress,
    }
}

#[derive(
  Clone, Debug, Hash, Eq, PartialEq, Serialize, Deserialize
)]
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
const Bill_Key_Size = AccountAddress.len_bytes() + BILL.length() + size_of(u64);

impl BillKey {
    pub fn encode(&self)->Result<Vec<u8>> {
        let mut encoded_key = self.0.as_ref().to_vec();
        encoded_key.extend_from_slice(BILL.as_bytes());
        encoded_key.write_u64::<BigEndian>(self.1).map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        Ok(encoded_key)
    }
    pub fn decode(&self, data:&[u8])->Result<Self> {
        ensure_len_eq(data.len(), Bill_Key_Size)?;
        let addr = AccountAddress::from(&data[..AccountAddress::len_bytes()]);
        let start_offset = AccountAddress::len_bytes() + BILL.len();
        let id = (&data[start_offset..]).read_u64::<BigEndian>().map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        Ok(Self(addr, id))
    }
}


#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn it_billkey_encode() {


	}
}
