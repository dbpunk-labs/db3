//
//
// build.rs
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

fn main() {
    tonic_build::configure()
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Serialize,serde::Deserialize)]")
        .compile(
            &[
                "proto/db3_base.proto",
                "proto/db3_session.proto",
                "proto/db3_mutation.proto",
                "proto/db3_mutation_v2.proto",
                "proto/db3_bill.proto",
                "proto/db3_account.proto",
                "proto/db3_node.proto",
                "proto/db3_database.proto",
                "proto/db3_database_v2.proto",
                "proto/db3_message.proto",
                "proto/db3_event.proto",
                "proto/db3_indexer.proto",
                "proto/db3_storage.proto",
                "proto/db3_rollup.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
