//
// storage_chain_minter.rs
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

use db3_crypto::{
    db3_address::DB3Address, db3_public_key::DB3PublicKey, signature_scheme::SignatureScheme,
};
use db3_error::{DB3Error, Result};
use db3_proto::db3_base_proto::{BroadcastMeta, ChainId, ChainRole};
use db3_proto::db3_mutation_proto::MintCreditsMutation;
use db3_sdk::mutation_sdk::MutationSDK;
use db3_storage::event_store::EventStore;
use elliptic_curve::{consts::U32, sec1::ToEncodedPoint};
use ethers::types::{RecoveryMessage, Signature, H256, U256};
use generic_array::GenericArray;
use hex;
use redb::Database;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use k256::{
    ecdsa::{
        recoverable::{Id as RecoveryId, Signature as RecoverableSignature},
        Error as K256SignatureError, Signature as K256Signature,
    },
    PublicKey as K256PublicKey,
};

pub struct StorageChainMinter {
    db: Arc<Database>,
    sdk: MutationSDK,
}

fn current_seconds() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => 0,
    }
}

impl StorageChainMinter {
    pub fn new(db: Arc<Database>, sdk: MutationSDK) -> StorageChainMinter {
        Self { db, sdk }
    }

    fn normalize_recovery_id(v: u8) -> u8 {
        match v {
            0 => 0,
            1 => 1,
            27 => 0,
            28 => 1,
            v if v >= 35 => ((v - 1) % 2) as _,
            _ => 4,
        }
    }

    fn get_db3_address(signature: &[u8], hash: &[u8]) -> Result<DB3Address> {
        let message_hash = H256::from_slice(hash);
        let v: u8 = signature[64];
        let recovery_id = RecoveryId::new(Self::normalize_recovery_id(v))
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let r = U256::from_big_endian(&signature[0..32]);
        let s = U256::from_big_endian(&signature[32..64]);
        let mut r_bytes = [0u8; 32];
        let mut s_bytes = [0u8; 32];
        r.to_big_endian(&mut r_bytes);
        s.to_big_endian(&mut s_bytes);
        let gar: &GenericArray<u8, U32> = GenericArray::from_slice(&r_bytes);
        let gas: &GenericArray<u8, U32> = GenericArray::from_slice(&s_bytes);
        let sig = K256Signature::from_scalars(*gar, *gas)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;

        let rsig = RecoverableSignature::new(&sig, recovery_id)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let verify_key = rsig
            .recover_verifying_key_from_digest_bytes(message_hash.as_ref().into())
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        let public_key = K256PublicKey::from(&verify_key);
        let public_key = public_key.to_encoded_point(/* compress = */ false);
        let public_key = public_key.as_bytes();
        let db3_public_key = DB3PublicKey::try_from_bytes(SignatureScheme::Secp256k1, &public_key)
            .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
        Ok(DB3Address::from(&db3_public_key))
    }

    async fn process_event(&self, chain_id: u32, block_id: u64) -> Result<()> {
        loop {
            let read_tx = self
                .db
                .begin_read()
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            let event_to_process = EventStore::get_unprocessed_event(read_tx)?;
            if let Some(event) = event_to_process {
                if event.block_id <= block_id && event.chain_id == chain_id {
                    let meta = BroadcastMeta {
                        //TODO get from network
                        nonce: current_seconds(),
                        //TODO use config
                        chain_id: ChainId::DevNet.into(),
                        //TODO use config
                        chain_role: ChainRole::StorageShardChain.into(),
                    };
                    //TODO add signature to the chain
                    let to_address = Self::get_db3_address(
                        event.signature.as_ref(),
                        event.tx_signed_hash.as_ref(),
                    )?;
                    let mutation = MintCreditsMutation {
                        chain_id,
                        block_id,
                        tx_id: event.transaction_id.to_vec(),
                        to: to_address.as_ref().to_vec(),
                        amount: event.amount,
                        meta: Some(meta),
                    };
                    let txid = self.sdk.submit_mint_credit_mutation(&mutation).await?;
                    let addr_str = format!("0x{}", hex::encode(to_address.as_ref()));
                    info!(
                        "send mint mutation with {} for event chain_id {} block_id {} to address {}",
                        txid.to_base64(),
                        chain_id,
                        block_id,
                        addr_str
                    );
                    let write_tx = self
                        .db
                        .begin_write()
                        .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
                    {
                        EventStore::store_event_progress(write_tx, &event)?;
                        info!(
                            "update event progress to chain_id {}, block_id {}",
                            chain_id, block_id
                        );
                        continue;
                    }
                }
                if event.block_id >= block_id && event.chain_id == chain_id {
                    warn!("block id {} has been processed and skip it", block_id);
                } else {
                    warn!("bad chain id expected {} but {}", event.chain_id, chain_id);
                }
                break;
            } else {
                warn!("no event to be processed");
                break;
            }
        }
        Ok(())
    }

    pub async fn start(&self, receiver: Receiver<(u32, u64)>) -> Result<()> {
        info!("minter is started");
        loop {
            let (chain_id, block_id) = receiver
                .recv()
                .map_err(|e| DB3Error::StoreEventError(format!("{e}")))?;
            self.process_event(chain_id, block_id).await?;
        }
    }
}
