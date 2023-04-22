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
use db3_crypto::id::BillId;
use db3_error::{DB3Error, Result};
use db3_proto::db3_bill_proto::Bill;
use db3_types::bill_key::BillKey;
use merkdb::proofs::{query::Query, Op as ProofOp};
use merkdb::{Merk, Op};
use prost::Message;
use std::collections::LinkedList;
use std::ops::Range;
use std::pin::Pin;

pub struct BillStore {}

impl BillStore {
    pub fn apply(db: Pin<&mut Merk>, bill_id: &BillId, bill: &Bill) -> Result<()> {
        let key = BillKey(bill_id);
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
                .map_err(|e| DB3Error::ApplyBillError(format!("{}", e)))?;
        }
        Ok(())
    }

    pub fn get_bill(db: Pin<&Merk>, bill_id: &BillId) -> Result<Option<Bill>> {
        let key = BillKey(bill_id);
        let encoded_key = key.encode()?;
        let values = db
            .get(encoded_key.as_ref())
            .map_err(|e| DB3Error::BillQueryError(format!("{}", e)))?;
        if let Some(v) = values {
            match Bill::decode(v.as_ref()) {
                Ok(a) => Ok(Some(a)),
                Err(e) => Err(DB3Error::BillQueryError(format!("{}", e))),
            }
        } else {
            Ok(None)
        }
    }

    pub fn get_block_bills(db: Pin<&Merk>, height: u64) -> Result<LinkedList<ProofOp>> {
        let (start, end) = BillId::get_block_range(height)?;
        let skey = BillKey(&start);
        let ekey = BillKey(&end);
        let range = Range {
            start: skey.encode()?,
            end: ekey.encode()?,
        };
        let mut query = Query::new();
        query.insert_range(range);
        let ops = db
            .execute_query(query)
            .map_err(|e| DB3Error::BillQueryError(format!("{}", e)))?;
        Ok(ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_proto::db3_bill_proto::BillType;
    use std::boxed::Box;
    use tempdir::TempDir;
    #[test]
    fn it_apply_bill_test() {
        let tmp_dir_path = TempDir::new("assign_partition").expect("create temp dir");
        let addr = vec![0; 20];
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let target_id: &str = "id";
        let bill = Bill {
            gas_fee: 1,
            block_id: 11,
            bill_type: BillType::BillForMutation.into(),
            time: 111,
            tx_id: target_id.as_bytes().to_vec(),
            owner: addr.to_vec(),
            to: vec![],
        };

        let bill_id = BillId::new(11, 111).unwrap();
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = BillStore::apply(db_m, &bill_id, &bill);

        assert!(result.is_ok());
        let bill = Bill {
            gas_fee: 1,
            block_id: 11,
            bill_type: BillType::BillForMutation.into(),
            time: 111,
            tx_id: target_id.as_bytes().to_vec(),
            owner: addr.to_vec(),
            to: vec![],
        };
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let bill_id = BillId::new(11, 1).unwrap();
        let result = BillStore::apply(db_m, &bill_id, &bill);
        assert!(result.is_ok());
        let result = BillStore::get_block_bills(db.as_ref(), 11);
        assert!(result.is_ok());
        let ops = result.unwrap();
        assert_eq!(ops.len(), 2);
    }
}
