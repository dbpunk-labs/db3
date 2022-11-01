//
// stroage_shard_node.rs
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

use std::boxed::Box;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

// the node config of db3 network
pub struct BlockState {
    last_block_height: i64,
    last_block_root_hash: Vec<u8>,
    db: Pin<Box<Merk>>,
    pending_mutation: Vec<(AccountAddress, Mutation, Bill)>,
    current_block_height: i64,
    current_block_root_hash: Vec<u8>,
}

pub struct NodeState {
    total_storage_bytes: Arc<AtomicU64>,
    total_mutations: Arc<AtomicU64>,
    total_query_sessions: Arc<AtomicU64>,
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn it_works() {
	}
}
