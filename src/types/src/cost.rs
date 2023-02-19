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
use db3_proto::db3_session_proto::QuerySessionInfo;

const C_CREATEDB_GAS_PRICE: u64 = 100; // unit in tai
const C_CREATECOLLECTION_GAS_PRICE: u64 = 100; // unit in tai
const C_CREATEINDEX_GAS_PRICE: u64 = 100; // unit in tai
const C_ADD_DOC_GAS_PRICE: u64 = 200; // unit in tai
const C_DEL_DOC_GAS_PRICE: u64 = 200; // unit in tai
const C_UPDATE_DOC_GAS_PRICE: u64 = 200; // unit in tai
const STORAGE_GAS_PRICE: u64 = 1; // per bytes
                                  //
const C_QUERY_OP_GAS_PRICE: u64 = 100;
#[derive(PartialEq, Eq, Debug)]
pub enum DbStoreOp {
    DbOp {
        create_db_ops: u64,
        create_collection_ops: u64,
        create_index_ops: u64,
        data_in_bytes: u64,
    },

    DocOp {
        add_doc_ops: u64,
        del_doc_ops: u64,
        update_doc_ops: u64,
        data_in_bytes: u64,
    },
}

impl DbStoreOp {
    pub fn update_data_size(&mut self, data_size: u64) {
        match self {
            DbStoreOp::DbOp {
                ref mut data_in_bytes,
                ..
            } => {
                *data_in_bytes = data_size;
            }
            DbStoreOp::DocOp {
                ref mut data_in_bytes,
                ..
            } => {
                *data_in_bytes = data_size;
            }
        }
    }
    pub fn get_data_size(&self) -> u64 {
        match self {
            DbStoreOp::DbOp { data_in_bytes, .. } => *data_in_bytes,
            DbStoreOp::DocOp { data_in_bytes, .. } => *data_in_bytes,
        }
    }
}

pub fn estimate_gas(ops: &DbStoreOp) -> u64 {
    match ops {
        DbStoreOp::DbOp {
            create_db_ops,
            create_collection_ops,
            create_index_ops,
            data_in_bytes,
        } => {
            C_CREATEDB_GAS_PRICE * create_db_ops
                + C_CREATECOLLECTION_GAS_PRICE * create_collection_ops
                + C_CREATEINDEX_GAS_PRICE * create_index_ops
                + STORAGE_GAS_PRICE * data_in_bytes
        }
        DbStoreOp::DocOp {
            add_doc_ops,
            del_doc_ops,
            update_doc_ops,
            data_in_bytes,
        } => {
            C_DEL_DOC_GAS_PRICE * add_doc_ops
                + C_ADD_DOC_GAS_PRICE * del_doc_ops
                + C_UPDATE_DOC_GAS_PRICE * update_doc_ops
                + STORAGE_GAS_PRICE * data_in_bytes
        }
    }
}

pub fn estimate_query_session_gas(query_session_info: &QuerySessionInfo) -> u64 {
    C_QUERY_OP_GAS_PRICE * query_session_info.query_count as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_session_proto::QuerySessionInfo;
    #[test]
    fn it_estimate_gas_doc_ops() {
        let doc_ops = DbStoreOp::DocOp {
            add_doc_ops: 1,
            del_doc_ops: 1,
            update_doc_ops: 1,
            data_in_bytes: 100,
        };
        let gas_fee = estimate_gas(&doc_ops);
        let target_fee = 200 + 200 + 200 + 100;
        assert_eq!(gas_fee, target_fee);
    }

    #[test]
    fn it_estimate_gas_db_ops() {
        let db_ops = DbStoreOp::DbOp {
            create_db_ops: 1,
            create_collection_ops: 1,
            create_index_ops: 1,
            data_in_bytes: 100,
        };
        let gas_fee = estimate_gas(&db_ops);
        let target_fee = 100 + 100 + 100 + 100;
        assert_eq!(gas_fee, target_fee);
    }

    #[test]
    fn it_query_session_estimate_gas() {
        let node_query_session_info = QuerySessionInfo {
            id: 1,
            start_time: Utc::now().timestamp(),
            query_count: 10,
        };
        let gas_fee = estimate_query_session_gas(&node_query_session_info);
        let target_fee = 1000;
        assert_eq!(gas_fee, target_fee);
    }
}
