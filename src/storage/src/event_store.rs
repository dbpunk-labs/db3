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
use redb::{TableDefinition, WriteTransaction};

const DEPOSIT_EVENT_TABLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("DEPOSIT_EVENT_TABLE");

pub struct EventStore {}

impl EventStore {
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
        let event = DepositEvent {
            chain_id: 1,
            sender: vec![0],
            amount: 1000_000_000,
            block_id: 10,
            transaction_id: vec![1],
            signature: vec![255],
        };
        let write_txn = db.begin_write().unwrap();
        let result = EventStore::store_deposit_event(write_txn, &event);
        assert!(result.is_ok());
        let write_txn = db.begin_write().unwrap();
        let result = EventStore::store_deposit_event(write_txn, &event);
        assert!(result.is_err());
    }
}
