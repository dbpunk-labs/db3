//
// bill_store.rs
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

use bytes::BytesMut;
use db3_error::{DB3Error, Result};
use db3_proto::db3_bill_proto::Bill;
use db3_types::bill_key::BillKey;
use ethereum_types::Address as AccountAddress;
use merk::{BatchEntry, Merk, Op};
use prost::Message;
use std::pin::Pin;
pub struct BillStore {}

impl BillStore {
    pub fn apply(db: Pin<&mut Merk>, account_addr: &AccountAddress, bill: &Bill) -> Result<()> {
        let key = BillKey(*account_addr, bill.bill_id);
        let encoded_key = key.encode()?;
        let mut buf = BytesMut::with_capacity(1024);
        bill.encode(&mut buf)
            .map_err(|e| DB3Error::ApplyBillError(format!("{}", e)))?;
        let buf = buf.freeze();
        //TODO avoid data copying
        let entry = (encoded_key, Op::Put(buf.to_vec()));
        unsafe {
            Pin::get_unchecked_mut(db)
                .apply(&[entry], &[])
                .map_err(|e| DB3Error::ApplyMutationError(format!("{}", e)))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_a_static_address;
    use db3_proto::db3_base_proto::{UnitType, Units};
    use db3_proto::db3_bill_proto::BillType;
    use std::boxed::Box;
    use tempdir::TempDir;
    #[test]
    fn it_apply_bill() {
        let tmp_dir_path = TempDir::new("assign_partition").expect("create temp dir");
        let addr = get_a_static_address();
        let mut merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let target_id: &str = "id";
        let bill = Bill {
            gas_fee: Some(Units {
                utype: UnitType::Db3.into(),
                amount: 1,
            }),
            block_height: 11,
            bill_id: 111,
            bill_type: BillType::BillForMutation.into(),
            time: 111,
            bill_target_id: target_id.as_bytes().to_vec(),
        };
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = BillStore::apply(db_m, &addr, &bill);
        assert!(result.is_ok());
    }
}
