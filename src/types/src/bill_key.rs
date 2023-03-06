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

use db3_crypto::id::BillId;
use db3_error::Result;
const BLOCK_BILL: &str = "/bl/";

pub struct BillKey<'a>(pub &'a BillId);
impl<'a> BillKey<'a> {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded_key = BLOCK_BILL.as_bytes().to_vec();
        encoded_key.extend_from_slice(self.0.as_ref());
        Ok(encoded_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_billkey_encode() -> Result<()> {
        let block_id: u64 = 1;
        let mutation_id: u16 = 1;
        let bill_id = BillId::new(block_id, mutation_id).unwrap();
        let bk = BillKey(&bill_id);
        let bk_encoded_key1 = bk.encode()?;
        let bill_id2 = BillId::new(block_id, mutation_id).unwrap();
        let bk = BillKey(&bill_id2);
        let bk_encoded_key2 = bk.encode()?;
        assert!(bk_encoded_key2.cmp(&bk_encoded_key1) == std::cmp::Ordering::Equal);
        let bill_id3 = BillId::new(1 as u64, 9 as u16).unwrap();
        let bk = BillKey(&bill_id3);
        let bk_encoded_key3 = bk.encode()?;
        assert!(bk_encoded_key3.cmp(&bk_encoded_key2) == std::cmp::Ordering::Greater);
        Ok(())
    }
}
