//
// ns_store.rs
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

use super::ns_key::NsKey;
use bytes::BytesMut;
use db3_error::{DB3Error, Result};
use db3_proto::db3_namespace_proto::Namespace;
use ethereum_types::Address as AccountAddress;
use merkdb::proofs::{query::Query, Op as ProofOp};
use merkdb::{BatchEntry, Merk, Op};
use prost::Message;
use std::collections::LinkedList;
use std::ops::Range;
use std::pin::Pin;

pub struct NsStore {}

impl NsStore {
    pub fn new() -> Self {
        Self {}
    }

    fn convert(ns: &Namespace, account_addr: &AccountAddress) -> Result<(BatchEntry, usize)> {
        let key = NsKey(*account_addr, ns.name.as_bytes().as_ref());
        let encoded_key = key.encode()?;
        let mut buf = BytesMut::with_capacity(1024 * 4);
        ns.encode(&mut buf)
            .map_err(|e| DB3Error::ApplyNamespaceError(format!("{}", e)))?;
        let buf = buf.freeze();
        let total_in_bytes = encoded_key.len() + buf.as_ref().len();
        Ok((
            (encoded_key, Op::Put(buf.as_ref().to_vec())),
            total_in_bytes,
        ))
    }

    pub fn apply(
        db: Pin<&mut Merk>,
        account_addr: &AccountAddress,
        namespace: &Namespace,
    ) -> Result<()> {
        let mut entries: Vec<BatchEntry> = Vec::new();
        let (batch_entry, _) = Self::convert(namespace, account_addr)?;
        entries.push(batch_entry);
        unsafe {
            Pin::get_unchecked_mut(db)
                .apply(&entries, &[])
                .map_err(|e| DB3Error::ApplyNamespaceError(format!("{}", e)))?;
        }
        Ok(())
    }

    pub fn get_my_ns_list(
        db: Pin<&Merk>,
        account_addr: &AccountAddress,
    ) -> Result<LinkedList<ProofOp>> {
        let start_key = NsKey(*account_addr, "".as_bytes().as_ref());
        let end_key = NsKey(*account_addr, "~~".as_bytes().as_ref());
        let range = Range {
            start: start_key.encode()?,
            end: end_key.encode()?,
        };
        let mut query = Query::new();
        query.insert_range(range);
        let ops = db
            .execute_query(query)
            .map_err(|e| DB3Error::QueryNamespaceError(format!("{}", e)))?;
        Ok(ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_a_static_address;
    use db3_proto::db3_base_proto::{Erc20Token, Price};
    use db3_proto::db3_namespace_proto::QueryPrice;
    use std::boxed::Box;
    use tempdir::TempDir;

    #[test]
    fn ns_store_smoke_test() {
        let tmp_dir_path = TempDir::new("assign_partition").expect("create temp dir");
        let addr = get_a_static_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let usdt = Erc20Token {
            symbal: "usdt".to_string(),
            units: vec!["cent".to_string(), "usdt".to_string()],
            scalar: vec![1, 10],
        };
        let price = Price {
            amount: 1,
            unit: "cent".to_string(),
            token: Some(usdt),
        };
        let query_price = QueryPrice {
            price: Some(price),
            query_count: 1000,
        };
        let ns = Namespace {
            name: "test1".to_string(),
            price: Some(query_price),
            ts: 1000,
            description: "test".to_string(),
            meta: None,
        };
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = NsStore::apply(db_m, &addr, &ns);
        assert!(result.is_ok());
        if let Ok(ops) = NsStore::get_my_ns_list(db.as_ref(), &addr) {
            assert_eq!(1, ops.len());
        } else {
            assert!(false);
        }
    }
}
