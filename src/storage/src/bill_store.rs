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
use merkdb::proofs::{query::Query, Op as ProofOp};
use merkdb::{Merk, Op};
use prost::Message;
use std::collections::LinkedList;
use std::ops::Range;
use std::pin::Pin;

pub struct BillStore {}

impl BillStore {
    pub fn apply(db: Pin<&mut Merk>, bill: &Bill) -> Result<()> {
        let key = BillKey(bill.block_height, bill.bill_id);
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

    pub fn scan(db: Pin<&Merk>, height: u64, start: u64, end: u64) -> Result<LinkedList<ProofOp>> {
        let skey = BillKey(height, start);
        let ekey = BillKey(height, end);
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
    use db3_base::get_a_static_address;
    use db3_proto::db3_base_proto::{UnitType, Units};
    use db3_proto::db3_bill_proto::BillType;
    use merkdb::proofs::{Decoder, Node};
    use std::boxed::Box;
    use tempdir::TempDir;
    #[test]
    fn it_apply_bill() {
        let tmp_dir_path = TempDir::new("assign_partition").expect("create temp dir");
        let addr = get_a_static_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
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
            query_addr: vec![],
            owner: addr.as_bytes().to_vec(),
        };
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = BillStore::apply(db_m, &bill);

        assert!(result.is_ok());
        let bill = Bill {
            gas_fee: Some(Units {
                utype: UnitType::Db3.into(),
                amount: 1,
            }),
            block_height: 11,
            bill_id: 1,
            bill_type: BillType::BillForMutation.into(),
            time: 111,
            bill_target_id: target_id.as_bytes().to_vec(),
            query_addr: vec![],
            owner: addr.as_bytes().to_vec(),
        };
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = BillStore::apply(db_m, &bill);
        assert!(result.is_ok());

        let skey = BillKey(11, 0).encode().unwrap();
        let ekey = BillKey(11, 200).encode().unwrap();
        let mut query = Query::new();
        let range = Range {
            start: skey,
            end: ekey,
        };
        query.insert_range(range);
        let result = db.as_ref().prove(query);
        if let Ok(r) = result {
            let mut decoder = Decoder::new(r.as_ref());
            loop {
                if let Some(Ok(op)) = decoder.next() {
                    match op {
                        ProofOp::Push(Node::KV(k, v)) => {
                            println!("k {:?} v {:?}", k, v);
                        }
                        ProofOp::Push(Node::KVHash(h)) => {
                            println!("kvhash {:?}", h);
                        }
                        ProofOp::Push(Node::Hash(h)) => {
                            println!("hash {:?}", h);
                        }
                        _ => {
                            println!("other");
                        }
                    }
                    continue;
                }
                break;
            }
        }
    }
}
