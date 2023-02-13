//
// cost.rs
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
use db3_proto::db3_base_proto::{UnitType, Units};
use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation};
use db3_proto::db3_session_proto::QuerySessionInfo;

const C_CREATEDB_GAS_PRICE: u64 = 10; // unit in tai
const C_CREATECOLLECTION_GAS_PRICE: u64 = 10; // unit in tai
const C_CREATEINDEX_GAS_PRICE: u64 = 10; // unit in tai
const C_WRITE_DOC_GAS_PRICE: u64 = 20; // unit in tai
const STORAGE_GAS_PRICE: u64 = 10; // per bytes

pub fn estimate_gas(mutation: &DatabaseMutation) -> Units {
    let mut gas: u64 = 0;
    let acount = DatabaseAction::from_i32(mutation.action);
    match action {
        Some(DatabaseAction::CreateDB) => {
            gas += C_CREATEDB_GAS_PRICE;
            for (key, collection) in mutation.collection_mutations.iter() {
                gas += key.len() * STORAGE_GAS_PRICE + C_CREATECOLLECTION_GAS_PRICE;
                for index in collection.index {
                    gas += C_CREATEINDEX_GAS_PRICE + index.name.len() * STORAGE_GAS_PRICE;
                }
            }
        }
        Some(DatabaseAction::AddCollection) => {
            for (key, collection) in mutation.collection_mutations.iter() {
                gas += key.len() * STORAGE_GAS_PRICE + C_CREATECOLLECTION_GAS_PRICE;
                for index in collection.index {
                    gas += C_CREATEINDEX_GAS_PRICE + index.name.len() * STORAGE_GAS_PRICE;
                }
            }
        }
        Some(DatabaseAction::AddDocument) => {
            for doc in mutation.document_mutations {
                let total_bytes = doc.document.iter().map(|d| d.len()).sum();
                gas += C_WRITE_DOC_GAS_PRICE + total_bytes * STORAGE_GAS_PRICE;
            }
        }
        _ => {
            todo!();
        }
    }
    Units {
        utype: UniType::Tai.into(),
        amount: gas,
    }
}

pub fn estimate_query_session_gas(query_session_info: &QuerySessionInfo) -> Units {
    let mut gas: u64 = 0;
    gas += query_session_info.query_count as u64 * COMPUTAION_GAS_PRICE;
    // TODO: estimate gas based on query count and weight
    Units {
        utype: UnitType::Tai.into(),
        amount: gas,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_session_proto::QuerySessionInfo;

    #[test]
    fn it_estimate_gas() {}

    #[test]
    fn it_query_session_estimate_gas() {
        let node_query_session_info = QuerySessionInfo {
            id: 1,
            start_time: Utc::now().timestamp(),
            query_count: 10,
        };

        let units = estimate_query_session_gas(&node_query_session_info);
        assert_eq!(1, units.utype);
        assert_eq!(100, units.amount);
    }
}
