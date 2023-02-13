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
const C_ADD_DOC_GAS_PRICE: u64 = 20; // unit in tai
const C_DEL_DOC_GAS_PRICE: u64 = 20; // unit in tai
const C_UPDATE_DOC_GAS_PRICE: u64 = 20; // unit in tai
const STORAGE_GAS_PRICE: u64 = 10; // per bytes
                                   //
pub enum DbStoreOp {
    DbOp {
        pub create_db_ops: u64,
        pub create_collection_ops: u64,
        pub create_index_ops: u64,
        pub data_in_bytes: u64,
    },

    DocOp {
        pub add_doc_ops: u64,
        pub del_doc_ops: u64,
        pub update_doc_ops: u64,
        pub data_in_bytes: u64,
    },
}

pub fn estimate_gas(ops: &DbStoreOp) -> Units {
    let mut gas: u64 = 0;
    match ops {
        DbStoreOp::DbOp {
            create_db_ops,
            create_collection_ops,
            create_index_ops,
            data_in_bytes,
        } => {
            gas += C_CREATEDB_GAS_PRICE * create_db_ops
                + C_CREATECOLLECTION_GAS_PRICE * create_collection_ops;
            gas += C_CREATEINDEX_GAS_PRICE * create_index_ops;
            gas += data_in_bytes * STORAGE_GAS_PRICE;
        }
        DbStoreOp::DocOp {
            add_doc_ops,
            del_doc_ops,
            update_doc_ops,
            data_in_bytes,
        } => {
            gas += add_doc_ops * C_ADD_DOC_GAS_PRICE + del_doc_ops * C_DEL_DOC_GAS_PRICE;
            gas += update_doc_ops * C_UPDATE_DOC_GAS_PRICE;
            gas += data_in_bytes * STORAGE_GAS_PRICE;
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
