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
use db3_base::{bson_util, strings};
use db3_crypto::db3_address::DB3Address;
use db3_crypto::id::{AccountId, DbId, DocumentId, TxId};
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_base_proto::{BroadcastMeta, ChainId, ChainRole};
use db3_proto::db3_database_proto::{Database, Document, Index};
use db3_proto::db3_mutation_proto::{
    CollectionMutation, DatabaseAction, DatabaseMutation, DocumentMutation,
};
use db3_proto::db3_node_proto::NetworkStatus;
use db3_sdk::{mutation_sdk::MutationSDK, store_sdk::StoreSDK};
use prettytable::{format, Table};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct DB3ClientContext {
    pub mutation_sdk: Option<MutationSDK>,
    pub store_sdk: Option<StoreSDK>,
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
    /// Show the database with an address
    #[clap(name = "show-db")]
    ShowDB {
        /// the address of database
        #[clap(long)]
        addr: String,
    },
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
        #[clap(long = "index")]
        index_list: Vec<String>,
    },
    #[clap(name = "show-collection")]
    ShowCollection {
        /// the address of database
        #[clap(long)]
        addr: String,
    },
    /// Create a document
    #[clap(name = "new-doc")]
    NewDocument {
        /// the address of database
        #[clap(long)]
        addr: String,
        /// the name of collection
        #[clap(long)]
        collection_name: String,
        /// the content of document
        #[clap(long)]
        documents: Vec<String>,
    },
    #[clap(name = "del-doc")]
    DeleteDocument {
        /// the address of database
        #[clap(long)]
        addr: String,
        /// the name of collection
        #[clap(long)]
        collection_name: String,
        /// the content of document
        #[clap(long)]
        ids: Vec<String>,
    },
    /// Get a document with given doc id
    #[clap(name = "get-doc")]
    GetDocument {
        /// the id(base64) of document
        #[clap(long)]
        id: String,
    },

    /// Show documents under a collection
    #[clap(name = "show-doc")]
    ShowDocument {
        /// the address of database
        #[clap(long)]
        addr: String,

        /// the name of collection
        #[clap(long)]
        collection_name: String,

        /// show document by key
        #[clap(long, default_value = "")]
        key: String,
    },
    #[clap(name = "show-account")]
    ShowAccount {},
    #[clap(name = "show-state")]
    ShowState {},
}

impl DB3ClientCommand {
    fn current_seconds() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_secs(),
            Err(_) => 0,
        }
    }
    fn show_document(documents: Vec<Document>) -> std::result::Result<Table, String> {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.set_titles(row!["id_base64", "owner", "document", "tx_id"]);
        let mut error_cnt = 0;
        for document in documents {
            if let Ok(id) = DocumentId::try_from_bytes(document.id.as_slice()) {
                if let Ok(doc) = bson_util::bytes_to_bson_document(document.doc) {
                    table.add_row(row![
                        id.to_base64(),
                        AccountId::try_from(document.owner.as_slice())
                            .unwrap()
                            .to_hex(),
                        format!("{:?}", doc),
                        TxId::try_from_bytes(document.tx_id.as_ref())
                            .unwrap()
                            .to_base64()
                    ]);
                } else {
                    error_cnt += 1;
                }
            } else {
                error_cnt += 1;
            }
        }
        if error_cnt > 0 {
            Err(format!(
                "An error occurs when attempting to show documents. Affected Rows {}",
                error_cnt
            ))
        } else {
            Ok(table)
        }
    }
    fn show_collection(database: &Database) -> std::result::Result<Table, String> {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.set_titles(row!["name", "index",]);
        for (_, collection) in &database.collections {
            let index_str: String = collection
                .index_list
                .iter()
                .map(|i| serde_json::to_string(&i).unwrap())
                .intersperse("\n ".to_string())
                .collect();
            table.add_row(row![collection.name, index_str]);
        }
        Ok(table)
    }

    fn show_database(database: &Database) -> std::result::Result<Table, String> {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.set_titles(row![
            "database address",
            "sender address",
            "related transactions",
            "collections"
        ]);
        let tx_list: String = database
            .tx
            .iter()
            .map(|tx| TxId::try_from_bytes(tx).unwrap().to_base64())
            .intersperse("\n ".to_string())
            .collect();
        let collections: String = database
            .collections
            .iter()
            .map(|(name, _)| name.to_string())
            .intersperse("\n ".to_string())
            .collect();
        let address_ref: &[u8] = database.address.as_ref();
        let sender_ref: &[u8] = database.sender.as_ref();
        table.add_row(row![
            DbId::try_from(address_ref).unwrap().to_hex(),
            AccountId::try_from(sender_ref).unwrap().to_hex(),
            tx_list,
            collections
        ]);
        Ok(table)
    }

    fn show_account(account: &Account, addr: &DB3Address) -> std::result::Result<Table, String> {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.set_titles(row![
            "address",
            "bills",
            "credits",
            "storage_used",
            "mutations",
            "session",
            "nonce"
        ]);
        table.add_row(row![
            AccountId::new(*addr).to_hex(),
            strings::units_to_readable_num_str(account.bills),
            strings::units_to_readable_num_str(account.credits),
            strings::bytes_to_readable_num_str(account.total_storage_in_bytes),
            account.total_mutation_count,
            account.total_session_count,
            account.nonce
        ]);
        Ok(table)
    }

    fn show_state(state: &NetworkStatus) -> std::result::Result<Table, String> {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.set_titles(row!["name", "state"]);
        table.add_row(row!["database".to_string(), state.total_database_count]);
        table.add_row(row!["collection".to_string(), state.total_collection_count]);
        table.add_row(row!["documemt".to_string(), state.total_document_count]);
        table.add_row(row!["account".to_string(), state.total_account_count]);
        table.add_row(row!["mutation".to_string(), state.total_mutation_count]);
        table.add_row(row!["session".to_string(), state.total_session_count]);
        table.add_row(row![
            "storage".to_string(),
            strings::bytes_to_readable_num_str(state.total_storage_in_bytes)
        ]);
        Ok(table)
    }

    pub async fn execute(self, ctx: &mut DB3ClientContext) -> std::result::Result<Table, String> {
        match self {
            DB3ClientCommand::Init {} => match KeyStore::recover_keypair() {
                Ok(ks) => ks.show_key(),
                Err(e) => Err(format!("{:?}", e)),
            },

            DB3ClientCommand::ShowKey {} => match KeyStore::recover_keypair() {
                Ok(ks) => ks.show_key(),
                Err(_) => Err(
                    "no key was found, you can use init command to create a new one".to_string(),
                ),
            },
            DB3ClientCommand::ShowState {} => {
                match ctx.store_sdk.as_ref().unwrap().get_state().await {
                    Ok(status) => Self::show_state(&status),
                    Err(_) => Err("fail to get account".to_string()),
                }
            }
            DB3ClientCommand::ShowAccount {} => match KeyStore::recover_keypair() {
                Ok(ks) => {
                    let addr = ks.get_address().unwrap();
                    match ctx.store_sdk.as_ref().unwrap().get_account(&addr).await {
                        Ok(account) => Self::show_account(&account, &addr),
                        Err(_) => Err("fail to get account".to_string()),
                    }
                }
                Err(_) => Err(
                    "no key was found, you can use init command to create a new one".to_string(),
                ),
            },
            DB3ClientCommand::NewCollection {
                addr,
                name,
                index_list,
            } => {
                //TODO validate the index
                let index_vec: Vec<Index> = index_list
                    .iter()
                    .map(|i| serde_json::from_str::<Index>(i.as_str()).unwrap())
                    .collect();
                let collection = CollectionMutation {
                    index: index_vec.to_owned(),
                    collection_name: name.to_string(),
                };
                //TODO check database id and collection name
                let db_id = DbId::try_from(addr.as_str()).unwrap();
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
                    collection_mutations: vec![collection],
                    db_address: db_id.as_ref().to_vec(),
                    action: DatabaseAction::AddCollection.into(),
                    document_mutations: vec![],
                };
                match ctx
                    .mutation_sdk
                    .as_ref()
                    .unwrap()
                    .submit_database_mutation(&dm)
                    .await
                {
                    Ok((_, tx_id)) => {
                        println!("send add collection done!");
                        let mut table = Table::new();
                        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                        table.set_titles(row!["tx_id"]);
                        table.add_row(row![tx_id.to_base64()]);
                        Ok(table)
                    }
                    Err(e) => Err(format!("fail to add collection: {e}")),
                }
            }
            DB3ClientCommand::GetDocument { id } => {
                match ctx
                    .store_sdk
                    .as_mut()
                    .unwrap()
                    .get_document(id.as_str())
                    .await
                {
                    Ok(Some(document)) => Self::show_document(vec![document]),
                    Ok(None) => Err("no document with target id".to_string()),
                    Err(e) => Err(format!("fail to get document with error {:?}", e)),
                }
            }
            DB3ClientCommand::ShowDocument {
                addr,
                collection_name,
                ..
            } => {
                // TODO(chenjing): construct index keys from json key string
                match ctx
                    .store_sdk
                    .as_mut()
                    .unwrap()
                    .list_documents(addr.as_ref(), collection_name.as_ref())
                    .await
                {
                    Ok(response) => Self::show_document(response.documents),
                    Err(e) => Err(format!("fail to show documents with error {:?}", e)),
                }
            }
            DB3ClientCommand::ShowCollection { addr } => {
                match ctx
                    .store_sdk
                    .as_mut()
                    .unwrap()
                    .get_database(addr.as_ref())
                    .await
                {
                    Ok(Some(database)) => Self::show_collection(&database),
                    Ok(None) => Err("no collection with target address".to_string()),
                    Err(e) => Err(format!("fail to show collections with error {e}")),
                }
            }

            DB3ClientCommand::ShowDB { addr } => {
                match ctx
                    .store_sdk
                    .as_mut()
                    .unwrap()
                    .get_database(addr.as_ref())
                    .await
                {
                    Ok(Some(database)) => Self::show_database(&database),
                    Ok(None) => Err(format!("no database with target address")),
                    Err(e) => Err(format!("fail to show database with error {e}")),
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
                    document_mutations: vec![],
                };
                match ctx
                    .mutation_sdk
                    .as_ref()
                    .unwrap()
                    .submit_database_mutation(&dm)
                    .await
                {
                    Ok((db_id, tx_id)) => {
                        let mut table = Table::new();
                        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                        table.set_titles(row!["database address", "transaction id"]);
                        table.add_row(row![db_id.to_hex(), tx_id.to_base64()]);
                        Ok(table)
                    }
                    Err(e) => Err(format!("fail to create database: {:?}", e)),
                }
            }
            DB3ClientCommand::NewDocument {
                addr,
                collection_name,
                documents,
            } => {
                //TODO validate the index existing in the document
                //TODO check database id and collection name
                let db_id = DbId::try_from(addr.as_str()).unwrap();
                let meta = BroadcastMeta {
                    //TODO get from network
                    nonce: Self::current_seconds(),
                    //TODO use config
                    chain_id: ChainId::DevNet.into(),
                    //TODO use config
                    chain_role: ChainRole::StorageShardChain.into(),
                };
                let bson_documents = documents
                    .iter()
                    .map(|x| bson_util::json_str_to_bson_bytes(x.as_str()).unwrap())
                    .collect();
                let document_mut = DocumentMutation {
                    collection_name,
                    documents: bson_documents,
                    ids: vec![],
                };
                let dm = DatabaseMutation {
                    meta: Some(meta),
                    action: DatabaseAction::AddDocument.into(),
                    db_address: db_id.as_ref().to_vec(),
                    document_mutations: vec![document_mut],
                    collection_mutations: vec![],
                };
                match ctx
                    .mutation_sdk
                    .as_ref()
                    .unwrap()
                    .submit_database_mutation(&dm)
                    .await
                {
                    Ok((_, tx_id)) => {
                        println!("send add document done");
                        let mut table = Table::new();
                        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                        table.set_titles(row!["transaction id"]);
                        table.add_row(row![tx_id.to_base64()]);
                        Ok(table)
                    }
                    Err(e) => Err(format!("fail to add document: {:?}", e)),
                }
            }
            DB3ClientCommand::DeleteDocument {
                addr,
                collection_name,
                ids,
            } => {
                if ids.is_empty() {
                    return Err("fail to delete with empty ids".to_string());
                }
                let db_id = DbId::try_from(addr.as_str()).unwrap();
                let meta = BroadcastMeta {
                    //TODO get from network
                    nonce: Self::current_seconds(),
                    //TODO use config
                    chain_id: ChainId::DevNet.into(),
                    //TODO use config
                    chain_role: ChainRole::StorageShardChain.into(),
                };
                let document_mut = DocumentMutation {
                    collection_name,
                    documents: vec![],
                    ids,
                };
                let dm = DatabaseMutation {
                    meta: Some(meta),
                    action: DatabaseAction::DeleteDocument.into(),
                    db_address: db_id.as_ref().to_vec(),
                    document_mutations: vec![document_mut],
                    collection_mutations: vec![],
                };
                match ctx
                    .mutation_sdk
                    .as_ref()
                    .unwrap()
                    .submit_database_mutation(&dm)
                    .await
                {
                    Ok((_, tx_id)) => {
                        println!("send delete document done");
                        let mut table = Table::new();
                        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                        table.set_titles(row!["transaction id"]);
                        table.add_row(row![tx_id.to_base64()]);
                        Ok(table)
                    }
                    Err(e) => Err(format!("fail to delete document: {:?}", e)),
                }
            }
        }
    }
}
