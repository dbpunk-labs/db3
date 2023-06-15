//
// indexer_impl.rs
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

use crate::mutation_utils::MutationUtil;
use db3_base::bson_util::bytes_to_bson_document;
use db3_crypto::db3_address::DB3Address;
use db3_error::{DB3Error, Result};
use db3_proto::db3_indexer_proto::indexer_node_server::IndexerNode;
use db3_proto::db3_indexer_proto::{
    IndexerStatus, RunQueryRequest, RunQueryResponse, ShowIndexerStatusRequest,
};
use db3_proto::db3_mutation_v2_proto::mutation::body_wrapper::Body;
use db3_proto::db3_mutation_v2_proto::MutationAction;
use db3_proto::db3_storage_proto::block_response::MutationWrapper;
use db3_proto::db3_storage_proto::event_message;
use db3_proto::db3_storage_proto::EventMessage as EventMessageV2;
use db3_sdk::store_sdk_v2::StoreSDKV2;
use db3_storage::db_store_v2::{DBStoreV2, DBStoreV2Config};
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct IndexerNodeImpl {
    db_store: DBStoreV2,
    network_id: u64,
}

impl IndexerNodeImpl {
    pub fn new(config: DBStoreV2Config, network_id: u64) -> Result<Self> {
        let db_store = DBStoreV2::new(config)?;
        Ok(Self {
            db_store,
            network_id,
        })
    }

    /// start standalone indexer block syncer
    /// 1. subscribe db3 event
    /// 2. handle event to sync db3 node block
    pub async fn start(&self, store_sdk: StoreSDKV2) -> Result<()> {
        info!("start indexer node ...");
        let mut stream = store_sdk
            .subscribe_event_message()
            .await
            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?
            .into_inner();
        info!("listen and handle event message");
        while let Some(event) = stream.message().await.unwrap() {
            match self.handle_event(event, &store_sdk).await {
                Err(e) => {
                    warn!("[IndexerBlockSyncer] handle event error: {:?}", e);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// handle event message
    async fn handle_event(&self, event: EventMessageV2, store_sdk: &StoreSDKV2) -> Result<()> {
        match event.event {
            Some(event_message::Event::BlockEvent(be)) => {
                info!(
                    "Receive BlockEvent: Block\t{}\tMutationCount\t{}",
                    be.block_id, be.mutation_count,
                );
                let response = store_sdk
                    .get_block_by_height(be.block_id)
                    .await
                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?
                    .into_inner();

                let mutations = response.mutations;
                debug!("Block mutations size: {:?}", mutations.len());
                self.parse_and_apply_mutations(&mutations)?;
            }
            _ => {}
        }
        Ok(())
    }
    fn parse_and_apply_mutations(&self, mutations: &Vec<MutationWrapper>) -> Result<()> {
        for mutation in mutations.iter() {
            let body = mutation.body.as_ref().unwrap();
            // validate the signature
            let (dm, address, nonce) =
                MutationUtil::unwrap_and_light_verify(&body.payload, body.signature.as_str())
                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
            let action = MutationAction::from_i32(dm.action).ok_or(DB3Error::WriteStoreError(
                "fail to convert action type".to_string(),
            ))?;
            let (block, order) = match &mutation.header {
                Some(header) => Ok((header.block_id, header.order_id)),
                _ => Err(DB3Error::WriteStoreError(
                    "invalid mutation header".to_string(),
                )),
            }?;
            match action {
                MutationAction::CreateDocumentDb => {
                    for body in dm.bodies {
                        if let Some(Body::DocDatabaseMutation(ref doc_db_mutation)) = &body.body {
                            let id = self
                                .db_store
                                .create_doc_database(
                                    &address,
                                    doc_db_mutation,
                                    nonce,
                                    self.network_id,
                                    block,
                                    order,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                "add database with addr {} from owner {}",
                                id.to_hex().as_str(),
                                address.to_hex().as_str()
                            );
                            break;
                        }
                    }
                }

                MutationAction::AddCollection => {
                    for (i, body) in dm.bodies.iter().enumerate() {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)
                            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                        if let Some(Body::CollectionMutation(ref col_mutation)) = &body.body {
                            self.db_store
                                .create_collection(
                                    &address,
                                    &db_addr,
                                    col_mutation,
                                    block,
                                    order,
                                    i as u16,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                    "add collection with db_addr {}, collection_name: {}, from owner {}",
                                    db_addr.to_hex().as_str(),
                                    col_mutation.collection_name.as_str(),
                                    address.to_hex().as_str()
                                );
                        }
                    }
                }
                MutationAction::UpdateDocument => {
                    for (_i, body) in dm.bodies.iter().enumerate() {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)
                            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                        if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                            let mut docs = Vec::<(String, i64)>::new();
                            for (j, buf) in doc_mutation.documents.iter().enumerate() {
                                let document = bytes_to_bson_document(buf.clone())
                                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                                let doc_str = document.to_string();
                                debug!("add document: {}", doc_str);
                                if doc_mutation.ids.len() <= j {
                                    warn!("no doc id for document {}", doc_str);
                                    break;
                                }
                                docs.push((doc_str, doc_mutation.ids[j]));
                            }
                            self.db_store
                                .update_docs(
                                    &db_addr,
                                    &address,
                                    doc_mutation.collection_name.as_str(),
                                    &docs,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                    "update documents with db_addr {}, collection_name: {}, from owner {}",
                                    db_addr.to_hex().as_str(),
                                    doc_mutation.collection_name.as_str(),
                                    address.to_hex().as_str()
                                );
                        }
                    }
                }
                MutationAction::DeleteDocument => {
                    for (_i, body) in dm.bodies.iter().enumerate() {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)
                            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                        if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                            self.db_store
                                .delete_docs(
                                    &db_addr,
                                    &address,
                                    doc_mutation.collection_name.as_str(),
                                    &doc_mutation.ids,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                    "delete documents with db_addr {}, collection_name: {}, from owner {}",
                                    db_addr.to_hex().as_str(),
                                    doc_mutation.collection_name.as_str(),
                                    address.to_hex().as_str()
                                );
                        }
                    }
                }

                MutationAction::AddDocument => {
                    for (_i, body) in dm.bodies.iter().enumerate() {
                        let db_address_ref: &[u8] = body.db_address.as_ref();
                        let db_addr = DB3Address::try_from(db_address_ref)
                            .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                        if let Some(Body::DocumentMutation(ref doc_mutation)) = &body.body {
                            let mut docs = Vec::<String>::new();
                            for buf in doc_mutation.documents.iter() {
                                let document = bytes_to_bson_document(buf.clone())
                                    .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                                let doc_str = document.to_string();
                                debug!("add document: {}", doc_str);
                                docs.push(doc_str);
                            }
                            let ids = self
                                .db_store
                                .add_docs(
                                    &db_addr,
                                    &address,
                                    doc_mutation.collection_name.as_str(),
                                    &docs,
                                )
                                .map_err(|e| DB3Error::WriteStoreError(format!("{e}")))?;
                            info!(
                                    "add documents with db_addr {}, collection_name: {}, from owner {}, ids: {:?}",
                                    db_addr.to_hex().as_str(),
                                    doc_mutation.collection_name.as_str(),
                                    address.to_hex().as_str(), ids,
                                );
                        }
                    }
                }
                _ => {
                    warn!("unsupported mutation action: {:?}", action);
                }
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl IndexerNode for IndexerNodeImpl {
    /// show indexer statuc
    async fn show_indexer_status(
        &self,
        _request: Request<ShowIndexerStatusRequest>,
    ) -> std::result::Result<Response<IndexerStatus>, Status> {
        Err(Status::internal("err".to_string()))
    }

    async fn run_query(
        &self,
        request: Request<RunQueryRequest>,
    ) -> std::result::Result<Response<RunQueryResponse>, Status> {
        let r = request.into_inner();
        let addr =
            DB3Address::from_hex(r.db.as_str()).map_err(|e| Status::internal(format!("{e}")))?;
        if let Some(q) = &r.query {
            info!("query str {} q {:?}", q.query_str, q);
            let documents = self
                .db_store
                .query_docs(&addr, r.col_name.as_str(), q)
                .map_err(|e| Status::internal(format!("{e}")))?;

            info!(
                "query str {} from collection {} in db {} with result len {}, parameters len {}",
                q.query_str,
                r.col_name.as_str(),
                r.db.as_str(),
                documents.len(),
                q.parameters.len()
            );
            Ok(Response::new(RunQueryResponse { documents }))
        } else {
            Err(Status::internal("no query provided".to_string()))
        }
    }
}
#[cfg(test)]
mod tests {}
