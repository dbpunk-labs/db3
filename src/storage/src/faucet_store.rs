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
use redb::{ReadTransaction, TableDefinition, WriteTransaction};

const FAUCET_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("FAUCET_TABLE");
const ADDRESS_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ADDRESS_TABLE");
const TOTAL_FUND_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("TOTAL_FUND_TABLE");

const EMPTY_VALUE: [u8; 0] = [0; 0];
static TOTAL_FUND_KEY: &str = "TOTAL_FUND_KEY";

pub struct FaucetStore {}

impl FaucetStore {
    pub fn init_table(tx: WriteTransaction) -> Result<()> {
        tx.open_table(FAUCET_TABLE)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        tx.open_table(ADDRESS_TABLE)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        tx.open_table(TOTAL_FUND_TABLE)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        tx.commit()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(())
    }

    pub fn get_state(tx: ReadTransaction) -> Result<(u64, u64)> {
        let addr_table = tx
            .open_table(ADDRESS_TABLE)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let total_unique_addr_count = addr_table
            .len()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let total_fund_table = tx
            .open_table(TOTAL_FUND_TABLE)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let value = total_fund_table
            .get(TOTAL_FUND_KEY.as_bytes())
            .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
        if let Some(fund_value) = value {
            let fixed_bytes: [u8; 8] = fund_value
                .value()
                .try_into()
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            let fund_amount = u64::from_be_bytes(fixed_bytes);
            Ok((total_unique_addr_count as u64, fund_amount))
        } else {
            Ok((total_unique_addr_count as u64, 0))
        }
    }

    pub fn store_record(tx: WriteTransaction, addr: &[u8], ts: u32, amount: u64) -> Result<()> {
        //
        // allow to request faucet in every hour
        //
        let key: Vec<u8> = faucet_key::build_faucet_key(addr, ts / (60 * 60 * 24))?;
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

        {
            let mut mut_table = tx
                .open_table(ADDRESS_TABLE)
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
            mut_table
                .insert(addr.as_ref(), EMPTY_VALUE.as_ref())
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
        }

        let fund = {
            let mut mut_table = tx
                .open_table(TOTAL_FUND_TABLE)
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
            let value = mut_table
                .insert(TOTAL_FUND_KEY.as_bytes(), amount.to_be_bytes().as_ref())
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
            let fund = match value {
                Some(old_value) => {
                    let fixed_bytes: [u8; 8] = old_value
                        .value()
                        .try_into()
                        .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
                    u64::from_be_bytes(fixed_bytes)
                }
                _ => 0,
            };
            fund
        };

        if fund > 0 {
            let mut mut_table = tx
                .open_table(TOTAL_FUND_TABLE)
                .map_err(|e| DB3Error::StoreFaucetError(format!("{e}")))?;
            let new_amount = fund + amount;
            mut_table
                .insert(TOTAL_FUND_KEY.as_bytes(), new_amount.to_be_bytes().as_ref())
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
            let read_txn = db.begin_read().unwrap();
            let (count, fund) = FaucetStore::get_state(read_txn).unwrap();
            assert_eq!(count, 0);
            assert_eq!(fund, 0);
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
        {
            let read_txn = db.begin_read().unwrap();
            let (count, fund) = FaucetStore::get_state(read_txn).unwrap();
            assert_eq!(count, 1);
            assert_eq!(fund, 10 * 1000_000_000);
        }
    }
}
