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
use db3_proto::db3_mutation_proto::{Mutation, MutationAction};
use db3_proto::db3_session_proto::QuerySessionInfo;

const COMPUTAION_GAS_PRICE: u64 = 10; // unit in tai
const STORAGE_GAS_PRICE: u64 = 10; // unit in tai

pub fn estimate_gas(mutation: &Mutation) -> Units {
    let mut gas: u64 = 0;
    gas += mutation.kv_pairs.len() as u64 * COMPUTAION_GAS_PRICE;
    for kv in &mutation.kv_pairs {
        let action = MutationAction::from_i32(kv.action);
        match action {
            Some(MutationAction::InsertKv) => {
                gas +=
                    (mutation.ns.len() + kv.key.len() + kv.value.len()) as u64 * STORAGE_GAS_PRICE;
            }
            _ => {}
        }
    }
    Units {
        utype: UnitType::Tai.into(),
        amount: gas as i64,
    }
}
pub fn estimate_query_session_gas(query_session_info: &QuerySessionInfo) -> Units {
    let mut gas: u64 = 0;
    gas += query_session_info.query_count as u64 * COMPUTAION_GAS_PRICE;
    // TODO: estimate gas based on query count and weight
    Units {
        utype: UnitType::Tai.into(),
        amount: gas as i64,
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use db3_proto::db3_base_proto::{ChainId, ChainRole};
    use db3_proto::db3_mutation_proto::{KvPair, MutationAction};
    use db3_proto::db3_session_proto::{QuerySessionInfo, SessionStatus};

    #[test]
    fn it_estimate_gas() {
        let kv = KvPair {
            key: "k1".as_bytes().to_vec(),
            value: "value1".as_bytes().to_vec(),
            action: MutationAction::InsertKv.into(),
        };
        let mutation = Mutation {
            ns: "my_twitter".as_bytes().to_vec(),
            kv_pairs: vec![kv],
            nonce: 1,
            chain_id: ChainId::MainNet.into(),
            chain_role: ChainRole::StorageShardChain.into(),
            gas_price: None,
            gas: 10,
        };
        let units = estimate_gas(&mutation);
        assert_eq!(1, units.utype);
        assert_eq!(190, units.amount);
    }

    #[test]
    fn it_query_session_estimate_gas() {
        let node_query_session_info = QuerySessionInfo {
            id: 1,
            start_time: Utc::now().timestamp(),
            query_count: 10,
            status: SessionStatus::Stop.into(),
        };

        let units = estimate_query_session_gas(&node_query_session_info);
        assert_eq!(1, units.utype);
        assert_eq!(100, units.amount);
    }
}
