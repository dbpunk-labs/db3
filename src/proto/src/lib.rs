//
//
// lib.rs
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

pub mod db3_base_proto {
    tonic::include_proto!("db3_base_proto");
}
pub mod db3_session_proto {
    tonic::include_proto!("db3_session_proto");
}
pub mod db3_mutation_proto {
    tonic::include_proto!("db3_mutation_proto");
}
pub mod db3_bill_proto {
    tonic::include_proto!("db3_bill_proto");
}
pub mod db3_account_proto {
    tonic::include_proto!("db3_account_proto");
}
pub mod db3_node_proto {
    tonic::include_proto!("db3_node_proto");
}
pub mod db3_database_proto {
    tonic::include_proto!("db3_database_proto");
}
pub mod db3_message_proto {
    tonic::include_proto!("db3_message_proto");
}

pub mod db3_event_proto {
    tonic::include_proto!("db3_event_proto");
}
pub mod db3_indexer_proto {
    tonic::include_proto!("db3_indexer_proto");
}

pub mod db3_storage_proto {
    tonic::include_proto!("db3_storage_proto");
}

pub mod db3_mutation_v2_proto {
    tonic::include_proto!("db3_mutation_v2_proto");
}
pub mod db3_database_v2_proto {
    tonic::include_proto!("db3_database_v2_proto");
}

pub mod db3_rollup_proto {
    tonic::include_proto!("db3_rollup_proto");
}
