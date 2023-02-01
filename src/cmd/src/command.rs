//
// command.rs
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

use clap::*;

use crate::keystore::KeyStore;
use db3_proto::db3_base_proto::{BroadcastMeta, ChainId, ChainRole};
use db3_proto::db3_mutation_proto::{DatabaseAction, DatabaseMutation};
use db3_sdk::mutation_sdk::MutationSDK;
use prettytable::{format, Table};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct DB3ClientContext {
    pub mutation_sdk: Option<MutationSDK>,
}

#[derive(Debug, Parser)]
#[clap(rename_all = "kebab-case")]
pub enum DB3ClientCommand {
    /// Init the client config file
    #[clap(name = "init")]
    Init {},
    /// Create a new key
    #[clap(name = "show-key")]
    ShowKey {},
    /// Create a database
    #[clap(name = "new-db")]
    NewDB {},
    /// Create a new collection
    #[clap(name = "new-collection")]
    NewCollection {
        /// the address of database
        #[clap(long)]
        addr: String,
        /// the name of collection
        #[clap(long)]
        name: String,
        /// the json style config of index
        #[clap(long)]
        config: String,
    },
}

impl DB3ClientCommand {
    fn current_seconds() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        }
    }
    pub async fn execute(self, ctx: &mut DB3ClientContext) {
        match self {
            DB3ClientCommand::Init {} => {
                if let Ok(_) = KeyStore::recover_keypair() {
                    println!("Init key successfully!");
                }
            }

            DB3ClientCommand::ShowKey {} => {
                if let Ok(ks) = KeyStore::recover_keypair() {
                    ks.show_key();
                }
            }

            DB3ClientCommand::NewDB {} => {
                let meta = BroadcastMeta {
                    //TODO get from network
                    nonce: Self::current_seconds(),
                    //TODO use config
                    chain_id: ChainId::DevNet.into(),
                    //TODO use config
                    chain_role: ChainRole::StorageShardChain.into(),
                };
                let dm = DatabaseMutation {
                    meta: Some(meta),
                    collection_mutations: vec![],
                    db_address: vec![],
                    action: DatabaseAction::CreateDb.into(),
                };
                if let Ok((db_id, tx_id)) = ctx
                    .mutation_sdk
                    .as_ref()
                    .unwrap()
                    .create_database(&dm)
                    .await
                {
                    let mut table = Table::new();
                    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                    table.set_titles(row!["database address", "transaction id"]);
                    table.add_row(row![db_id.to_hex(), tx_id.to_base64()]);
                    table.printstd();
                } else {
                    println!("fail to create database");
                }
            }
            _ => {}
        }
    }
}
