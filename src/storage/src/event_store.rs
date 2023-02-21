//
// event_store.rs
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

use crate::event_key;
use bytes::BytesMut;
use db3_error::{DB3Error, Result};
use db3_proto::db3_message_proto::DepositEvent;
use prost::Message;
use redb::ReadableTable;
use redb::{ReadTransaction, TableDefinition, WriteTransaction};

const DEPOSIT_EVENT_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("DEPOSIT_EVENT_TABLE");

const DEPOSIT_EVENT_PROGRESS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("DEPOSIT_EVENT_PROGRESS");

const EMPTY_VALUE: [u8; 0] = [0; 0];

pub struct EventStore {}

impl EventStore {
    pub fn init_table(tx: WriteTransaction) -> Result<()> {
        tx.open_table(DEPOSIT_EVENT_TABLE)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        tx.open_table(DEPOSIT_EVENT_PROGRESS)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        tx.commit()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(())
    }
    ///
    /// store a new DepositEvent. Return DB3Error if the DepositEvent does exist
    ///
    pub fn store_deposit_event(tx: WriteTransaction, event: &DepositEvent) -> Result<()> {
        let key: Vec<u8> = event_key::build_event_key(
            event_key::EventType::DepositEvent,
            event.chain_id,
            event.block_id,
            event.transaction_id.as_ref(),
        )?;
        let key_ref: &[u8] = key.as_ref();
        //TODO validate the event
        {
            let read_table = tx
                .open_table(DEPOSIT_EVENT_TABLE)
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            let value = read_table
                .get(key_ref)
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            if value.is_some() {
                return Err(DB3Error::StoreEventError(
                    "deposit event exists".to_string(),
                ));
            }
        }
        {
            let mut mut_table = tx
                .open_table(DEPOSIT_EVENT_TABLE)
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            let mut buf = BytesMut::with_capacity(1024 * 4);
            event
                .encode(&mut buf)
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            let buf = buf.freeze();
            mut_table
                .insert(key_ref, buf.as_ref())
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        }
        tx.commit()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(())
    }

    pub fn store_event_progress(tx: WriteTransaction, event: &DepositEvent) -> Result<()> {
        let key: Vec<u8> = event_key::build_event_key(
            event_key::EventType::DepositEvent,
            event.chain_id,
            event.block_id,
            event.transaction_id.as_ref(),
        )?;
        {
            let key_ref: &[u8] = key.as_ref();
            let mut mut_table = tx
                .open_table(DEPOSIT_EVENT_PROGRESS)
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            mut_table
                .insert(key_ref, EMPTY_VALUE.as_ref())
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        }
        tx.commit()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(())
    }

    pub fn get_unprocessed_event(tx: ReadTransaction) -> Result<Option<DepositEvent>> {
        let progress = tx
            .open_table(DEPOSIT_EVENT_PROGRESS)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let events = tx
            .open_table(DEPOSIT_EVENT_TABLE)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let progress_len = progress
            .len()
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        if progress_len <= 0 {
            let mut it = events
                .iter()
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            let first = it.next();
            if let Some((_, ref v)) = first {
                match DepositEvent::decode(v.value().as_ref()) {
                    Ok(event) => return Ok(Some(event)),
                    Err(e) => return Err(DB3Error::StoreEventError(format!("{e}"))),
                }
            }
        } else {
            let p_it = progress
                .iter()
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            let last = p_it.rev().next();
            if let Some((ref key, _)) = last {
                let (event_type, chaid_id, block_id) =
                    event_key::decode_event_key(key.value().as_ref())?;
                let (start, end) =
                    event_key::build_event_key_range(event_type, chaid_id, block_id + 1)?;
                let range: std::ops::Range<&[u8]> = std::ops::Range {
                    start: start.as_ref(),
                    end: end.as_ref(),
                };
                let mut range_it = events
                    .range(range)
                    .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
                let first = range_it.next();
                if let Some((_, ref v)) = first {
                    match DepositEvent::decode(v.value().as_ref()) {
                        Ok(event) => return Ok(Some(event)),
                        Err(e) => return Err(DB3Error::StoreEventError(format!("{e}"))),
                    }
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::Database;
    use tempdir::TempDir;
    #[test]
    fn test_store_event() {
        let tmp_dir_path = TempDir::new("event_store_test").expect("create temp dir");
        let db_path = tmp_dir_path.path().join("event_store.db");
        let db = Database::create(db_path.as_path().to_str().unwrap()).unwrap();
        {
            let write_txn = db.begin_write().unwrap();
            EventStore::init_table(write_txn).unwrap();
        }
        {
            let read_tx = db.begin_read().unwrap();
            let deposit_event = EventStore::get_unprocessed_event(read_tx).unwrap();
            assert!(deposit_event.is_none());
        }

        {
            let event = DepositEvent {
                chain_id: 1,
                sender: vec![0],
                amount: 1000_000_000,
                block_id: 10,
                transaction_id: vec![1],
                signature: vec![255],
                tx_signed_hash: vec![0],
            };
            let write_txn = db.begin_write().unwrap();
            let result = EventStore::store_deposit_event(write_txn, &event);
            assert!(result.is_ok());
            let write_txn = db.begin_write().unwrap();
            let result = EventStore::store_deposit_event(write_txn, &event);
            assert!(result.is_err());
        }

        {
            let event = DepositEvent {
                chain_id: 1,
                sender: vec![0],
                amount: 1000_000_000,
                block_id: 11,
                transaction_id: vec![1],
                signature: vec![255],
                tx_signed_hash: vec![0],
            };
            let write_txn = db.begin_write().unwrap();
            let result = EventStore::store_deposit_event(write_txn, &event);
            assert!(result.is_ok());
            let write_txn = db.begin_write().unwrap();
            let result = EventStore::store_deposit_event(write_txn, &event);
            assert!(result.is_err());
        }
        {
            let read_tx = db.begin_read().unwrap();
            let table = read_tx.open_table(DEPOSIT_EVENT_TABLE).unwrap();
            let it = table.iter().unwrap();
            let last = it.rev().next();
            if let Some((_, ref v)) = last {
                match DepositEvent::decode(v.value().as_ref()) {
                    Ok(a) => {
                        assert_eq!(11, a.block_id);
                    }
                    Err(e) => {
                        assert!(false);
                    }
                }
            }
        }
        {
            let read_tx = db.begin_read().unwrap();
            let deposit_event = EventStore::get_unprocessed_event(read_tx).unwrap();
            assert!(deposit_event.is_some());
            if let Some(e) = deposit_event {
                assert_eq!(e.block_id, 10)
            }
        }
        {
            let write_txn = db.begin_write().unwrap();
            let event = DepositEvent {
                chain_id: 1,
                sender: vec![0],
                amount: 1000_000_000,
                block_id: 10,
                transaction_id: vec![1],
                signature: vec![255],
                tx_signed_hash: vec![0],
            };
            let result = EventStore::store_event_progress(write_txn, &event);
            assert!(result.is_ok());
            let read_tx = db.begin_read().unwrap();
            let deposit_event = EventStore::get_unprocessed_event(read_tx).unwrap();
            assert!(deposit_event.is_some());
            if let Some(e) = deposit_event {
                assert_eq!(e.block_id, 11)
            }
        }
    }
}
