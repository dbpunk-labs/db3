//
// faucet_store.rs
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

use crate::faucet_key;
use bytes::BytesMut;
use db3_error::{DB3Error, Result};
use db3_proto::db3_faucet_proto::FaucetRecord;
use prost::Message;
use redb::ReadableTable;
use redb::{TableDefinition, WriteTransaction};

const FAUCET_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("FAUCET_TABLE");

pub struct FaucetStore {}

impl FaucetStore {
    pub fn init_table(tx: WriteTransaction) -> Result<()> {
        tx.open_table(FAUCET_TABLE)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        tx.commit()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(())
    }

    pub fn store_record(tx: WriteTransaction, addr: &[u8], ts: u32, amount: u64) -> Result<()> {
        //
        // allow to request faucet in every hour
        //
        let key: Vec<u8> = faucet_key::build_faucet_key(addr, ts / 60)?;
        let key_ref: &[u8] = key.as_ref();
        {
            let read_table = tx
                .open_table(FAUCET_TABLE)
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
            let value = read_table
                .get(key_ref)
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
            if value.is_some() {
                return Err(DB3Error::StoreFaucetError(
                    "request faucet is not allowed".to_string(),
                ));
            }
        }

        {
            let mut mut_table = tx
                .open_table(FAUCET_TABLE)
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
            let record = FaucetRecord {
                addr: addr.to_vec(),
                ts,
                amount,
            };
            let mut buf = BytesMut::with_capacity(1024 * 4);
            record
                .encode(&mut buf)
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
            let buf = buf.freeze();
            mut_table
                .insert(key_ref, buf.as_ref())
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
        }
        tx.commit()
            .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::Database;
    use tempdir::TempDir;
    #[test]
    fn faucet_store_smoke_test() {
        let tmp_dir_path = TempDir::new("faucet_store_test").expect("create temp dir");
        let db_path = tmp_dir_path.path().join("faucet_store.db");
        let db = Database::create(db_path.as_path().to_str().unwrap()).unwrap();
        {
            let write_txn = db.begin_write().unwrap();
            FaucetStore::init_table(write_txn).unwrap();
        }
        {
            let addr: [u8; 20] = [1; 20];
            let write_txn = db.begin_write().unwrap();
            let result =
                FaucetStore::store_record(write_txn, &addr as &[u8], 1677040755, 10 * 1000_000_000);
            assert!(result.is_ok());
            let write_txn = db.begin_write().unwrap();
            let result =
                FaucetStore::store_record(write_txn, &addr as &[u8], 1677040755, 10 * 1000_000_000);
            assert!(result.is_err());
        }
    }
}
