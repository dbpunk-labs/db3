use crate::node_storage::NodeStorage;
use db3_crypto::{db3_address::DB3Address as AccountAddress, id::TxId};
use db3_proto::db3_event_proto::event_message;
use db3_proto::db3_event_proto::mutation_event::MutationEventStatus;
use db3_proto::db3_event_proto::EventMessage;
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
        let mut stream = self
            .store_sdk
            .subscribe_event_message(true)
            .await?
            .into_inner();
        println!("listen and handle event message");
        while let Some(event) = stream.message().await? {
            match self.handle_event(event).await {
                Err(e) => {
                    println!("[Indexer] handle event error: {:?}", e);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// handle event message
    async fn handle_event(&mut self, event: EventMessage) -> std::result::Result<(), Status> {
        match event.event {
            Some(event_message::Event::MutationEvent(me)) => {
                if let Some(status_type) = MutationEventStatus::from_i32(me.status) {
                    println!(
                        "[Indexer] receive mutation:{:?}\t{}\t{}\t{}\t{}\t{:?}",
                        status_type, me.height, me.sender, me.to, me.hash, me.collections
                    );
                } else {
                    println!(
                        "[Indexer] receive mutation: unknown\t{}\t{}\t{}\t{}\t{:?}",
                        me.height, me.sender, me.to, me.hash, me.collections
                    );
                }
            }
            Some(event_message::Event::BlockEvent(be)) => {
                println!(
                    "Block\t{}\t0x{}\t0x{}\t{}",
                    be.height,
                    hex::encode(be.block_hash),
                    hex::encode(be.app_hash),
                    be.gas
                );
            }
            _ => {}
        }
        Ok(())
    }
}
