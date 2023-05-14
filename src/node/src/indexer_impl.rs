use crate::node_storage::NodeStorage;
use db3_cmd::command::DB3ClientContext;
use db3_crypto::{db3_address::DB3Address as AccountAddress, id::TxId};
use db3_proto::db3_event_proto::event_message;
use db3_proto::db3_mutation_proto::DatabaseMutation;
use db3_proto::db3_session_proto::QuerySessionInfo;
use db3_sdk::store_sdk::StoreSDK;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tonic::Status;

pub struct IndexerImpl {
    store_sdk: StoreSDK,
    node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>,
    pending_query_session:
        Arc<Mutex<Vec<(AccountAddress, AccountAddress, TxId, QuerySessionInfo)>>>,
    pending_databases: Arc<Mutex<Vec<(AccountAddress, DatabaseMutation, TxId)>>>,
}

impl IndexerImpl {
    pub fn new(store_sdk: StoreSDK, node_store: Arc<Mutex<Pin<Box<NodeStorage>>>>) -> Self {
        Self {
            store_sdk,
            node_store,
            pending_query_session: Arc::new(Mutex::new(Vec::new())),
            pending_databases: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// start standalone indexer
    /// 1. subscribe db3 event
    /// 2. handle event
    pub async fn start(&mut self) -> std::result::Result<(), Status> {
        println!("[Indexer] start indexer ...");
        println!("[Indexer] subscribe event from db3 network");
        // let mut stjoream = &self.db3_ctx.store_sdk.unwrap()
        //     .subscribe_event_message(true).await?.into_inner();
        let mut stream = self
            .store_sdk
            .subscribe_event_message(true)
            .await?
            .into_inner();
        println!("listen and handle event message");
        while let Some(event) = stream.message().await? {
            match event.event {
                Some(event_message::Event::MutationEvent(me)) => {
                    println!(
                        "[Indexer] receive mutation\t{}\t{}\t{}\t{}\t{:?}",
                        me.height, me.sender, me.to, me.hash, me.collections
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }
}
