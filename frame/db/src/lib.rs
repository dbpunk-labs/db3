// This file is part of Substrate.

// Copyright (C) 2017-2022 Parity Technologies (UK) Ltd.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Transaction storage pallet. Indexes transactions and manages storage proofs.

// Ensure we're `no_std` when compiling for Wasm.
#![cfg_attr(not(feature = "std"), no_std)]
mod benchmarking;
pub mod weights;

#[cfg(test)]
mod mock;
#[cfg(test)]
mod tests;

use codec::{Decode, Encode, MaxEncodedLen};
use frame_support::{
    dispatch::{Dispatchable, GetDispatchInfo},
    traits::{Currency, OnUnbalanced, ReservableCurrency},
    BoundedBTreeMap, BoundedBTreeSet, BoundedVec,
};
use frame_system::{
    self as system,
    offchain::{
        AppCrypto, CreateSignedTransaction, SendSignedTransaction, SendUnsignedTransaction, Signer,
        SubmitTransaction,
    },
};
use sp_io::offchain_index;
use sp_runtime::traits::{BlakeTwo256, Hash, One, Saturating, StaticLookup, Zero};
use sp_std::{prelude::*, result, str};
use sp_transaction_storage_proof::{
    encode_index, random_chunk, InherentError, TransactionStorageProof, CHUNK_SIZE,
    INHERENT_IDENTIFIER,
};

use hex;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json;
use sp_core::crypto::KeyTypeId;
use sp_runtime::{
    offchain::{
        http,
        storage::{MutateStorageError, StorageRetrievalError, StorageValueRef},
        Duration,
    },
    transaction_validity::{InvalidTransaction, TransactionValidity, ValidTransaction},
    RuntimeDebug,
};

type AccountIdLookupOf<T> = <<T as frame_system::Config>::Lookup as StaticLookup>::Source;

/// A type alias for the balance type from this pallet's point of view.
type BalanceOf<T> =
    <<T as Config>::Currency as Currency<<T as frame_system::Config>::AccountId>>::Balance;
type NegativeImbalanceOf<T> = <<T as Config>::Currency as Currency<
    <T as frame_system::Config>::AccountId,
>>::NegativeImbalance;

// Re-export pallet items so that they can be accessed from the crate namespace.
pub use pallet::*;
pub use weights::WeightInfo;

/// Maximum bytes that can be stored in one transaction.
// Setting higher limit also requires raising the allocator limit.
pub const DEFAULT_MAX_TRANSACTION_SIZE: u32 = 8 * 1024 * 1024;
pub const DEFAULT_MAX_BLOCK_TRANSACTIONS: u32 = 512;
const SQL_KEY: &[u8] = b"sql_key";

/// State data for a stored transaction.
#[derive(
    Encode,
    Decode,
    Clone,
    sp_runtime::RuntimeDebug,
    PartialEq,
    Eq,
    scale_info::TypeInfo,
    MaxEncodedLen,
)]
pub struct TransactionInfo {
    /// Chunk trie root.
    chunk_root: <BlakeTwo256 as Hash>::Output,
    /// Plain hash of indexed data.
    content_hash: <BlakeTwo256 as Hash>::Output,
    /// Size of indexed data in bytes.
    size: u32,
    /// Total number of chunks added in the block with this transaction. This
    /// is used find transaction info by block chunk index using binary search.
    block_chunks: u32,
}

fn num_chunks(bytes: u32) -> u32 {
    ((bytes as u64 + CHUNK_SIZE as u64 - 1) / CHUNK_SIZE as u64) as u32
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SQLInput<'a> {
    query: &'a str,
    account: &'a str,
    req_id: &'a str,
}

#[derive(Encode, Decode, Clone, PartialEq, Eq, RuntimeDebug, scale_info::TypeInfo)]
pub struct InputPayload {
    data: Vec<u8>,
}

pub const KEY_TYPE: KeyTypeId = KeyTypeId(*b"SQLH");

/// Based on the above `KeyTypeId` we need to generate a pallet-specific crypto type wrappers.
/// We can use from supported crypto kinds (`sr25519`, `ed25519` and `ecdsa`) and augment
/// the types with this pallet-specific identifier.
pub mod crypto {
    use super::KEY_TYPE;
    use sp_core::sr25519::Signature as Sr25519Signature;
    use sp_runtime::{
        app_crypto::{app_crypto, sr25519},
        traits::Verify,
        MultiSignature, MultiSigner,
    };
    app_crypto!(sr25519, KEY_TYPE);

    pub struct TestAuthId;

    impl frame_system::offchain::AppCrypto<MultiSigner, MultiSignature> for TestAuthId {
        type RuntimeAppPublic = Public;
        type GenericSignature = sp_core::sr25519::Signature;
        type GenericPublic = sp_core::sr25519::Public;
    }

    // implemented for mock runtime in test
    impl frame_system::offchain::AppCrypto<<Sr25519Signature as Verify>::Signer, Sr25519Signature>
        for TestAuthId
    {
        type RuntimeAppPublic = Public;
        type GenericSignature = sp_core::sr25519::Signature;
        type GenericPublic = sp_core::sr25519::Public;
    }
}

#[frame_support::pallet]
pub mod pallet {
    use super::*;
    use frame_support::pallet_prelude::*;
    use frame_system::pallet_prelude::*;

    #[pallet::config]
    pub trait Config: CreateSignedTransaction<Call<Self>> + frame_system::Config {
        type AuthorityId: AppCrypto<Self::Public, Self::Signature>;
        /// The overarching event type.
        type Event: From<Event<Self>> + IsType<<Self as frame_system::Config>::Event>;
        /// A dispatchable call.
        type Call: Parameter
            + Dispatchable<Origin = Self::Origin>
            + GetDispatchInfo
            + From<frame_system::Call<Self>>;
        /// The currency trait.
        type Currency: ReservableCurrency<Self::AccountId>;
        /// Handler for the unbalanced decrease when fees are burned.
        type FeeDestination: OnUnbalanced<NegativeImbalanceOf<Self>>;
        /// Weight information for extrinsics in this pallet.
        type WeightInfo: WeightInfo;
        /// Maximum number of indexed transactions in the block.
        type MaxBlockTransactions: Get<u32>;
        /// Maximum data set in a single transaction in bytes.
        type MaxTransactionSize: Get<u32>;
    }

    #[pallet::error]
    pub enum Error<T> {
        /// Insufficient account balance.
        InsufficientFunds,
        /// Invalid configuration.
        NotConfigured,
        /// Renewed extrinsic is not found.
        RenewedNotFound,
        /// Attempting to store empty transaction
        EmptyTransaction,
        /// Proof was not expected in this block.
        UnexpectedProof,
        /// Proof failed verification.
        InvalidProof,
        /// Missing storage proof.
        MissingProof,
        /// Unable to verify proof becasue state data is missing.
        MissingStateData,
        /// Double proof check in the block.
        DoubleCheck,
        /// Storage proof was not checked in the block.
        ProofNotChecked,
        /// Transaction is too large.
        TransactionTooLarge,
        /// Too many transactions in the block.
        TooManyTransactions,
        /// Attempted to call `store` outside of block execution.
        BadContext,
    }

    #[pallet::pallet]
    #[pallet::generate_store(pub(super) trait Store)]
    pub struct Pallet<T>(_);

    #[pallet::hooks]
    impl<T: Config> Hooks<BlockNumberFor<T>> for Pallet<T> {
        fn on_initialize(n: T::BlockNumber) -> Weight {
            // Drop obsolete roots. The proof for `obsolete` will be checked later
            // in this block, so we drop `obsolete` - 1.
            let period = <StoragePeriod<T>>::get();
            let obsolete = n.saturating_sub(period.saturating_add(One::one()));
            if obsolete > Zero::zero() {
                <Transactions<T>>::remove(obsolete);
                <ChunkCount<T>>::remove(obsolete);
            }
            PendingSQL::<T>::kill();
            // 2 writes in `on_initialize` and 2 writes + 2 reads in `on_finalize`
            T::DbWeight::get().reads_writes(2, 4)
        }

        fn on_finalize(n: T::BlockNumber) {
            assert!(
                <ProofChecked<T>>::take() || {
                    // Proof is not required for early or empty blocks.
                    let number = <frame_system::Pallet<T>>::block_number();
                    let period = <StoragePeriod<T>>::get();
                    let target_number = number.saturating_sub(period);
                    target_number.is_zero() || <ChunkCount<T>>::get(target_number) == 0
                },
                "Storage proof must be checked once in the block"
            );
            // Insert new transactions
            let transactions = <BlockTransactions<T>>::take();
            let total_chunks = transactions.last().map_or(0, |t| t.block_chunks);
            if total_chunks != 0 {
                <ChunkCount<T>>::insert(n, total_chunks);
                <Transactions<T>>::insert(n, transactions);
            }
        }

        fn offchain_worker(_n: T::BlockNumber) {
            //TODO use batch to sumit transaction
            for (id, sql_data, req_id, ns) in PendingSQL::<T>::get() {
                let ns_raw = ns.to_vec();
                let raw_data = sql_data.to_vec();
                let query = str::from_utf8(&raw_data).unwrap_or("error");
                let raw_id_data = req_id.to_vec();
                let req_id = str::from_utf8(&raw_id_data).unwrap_or("error");
                let mut data = id.encode();
                data.extend(ns_raw.iter());
                let account = hex::encode_upper(data);
                log::info!("process sql {} offchain from acc {} ", query, account);
                Self::run_and_send_signed_payload(query, &account, req_id).unwrap();
            }
        }
    }

    #[pallet::call]
    impl<T: Config> Pallet<T> {
        #[pallet::weight(10000)]
        pub fn submit_unsigned_result(origin: OriginFor<T>, data: Vec<u8>) -> DispatchResult {
            ensure_none(origin)?;
            Self::deposit_event(Event::SQLResult(data));
            Ok(())
        }

        #[pallet::weight(10000)]
        pub fn submit_result(origin: OriginFor<T>, data: InputPayload) -> DispatchResult {
            ensure_signed(origin)?;
            Self::deposit_event(Event::SQLResult(data.data));
            Ok(())
        }

        #[pallet::weight(10000)]
        pub fn create_ns(origin: OriginFor<T>, 
            ns: Vec<u8>,
            req_id: Vec<u8>,) -> DispatchResult {
            let owner = ensure_signed(origin)?;
            if !<NsOwners<T>>::contains_key(&owner) {
                let bset: BoundedBTreeSet<
                    BoundedVec<u8, T::MaxBlockTransactions>,
                    T::MaxBlockTransactions,
                > = BoundedBTreeSet::new();
                <NsOwners<T>>::insert(owner.clone(), bset);
            } 
            // add owner
            <NsOwners<T>>::mutate(&owner, |bset| {
                let ns_name: BoundedVec<_, _> = ns
                    .clone()
                    .try_into()
                    .map_err(|()| Error::<T>::TooManyTransactions)
                    .unwrap();
                
                if let Some(b) = bset {
                    if let Ok(_) = b.try_insert(ns_name) {
                    }
                }
            });
            Self::deposit_event(
                Event::GeneralResultEvent{
                    status : 0,
                    msg : "ok".as_bytes().to_vec(),
                    req_id: req_id
                });
            Ok(())
        }

        #[pallet::weight(10000)]
        pub fn create_ns_and_add_delegate(
            origin: OriginFor<T>,
            delegate: AccountIdLookupOf<T>,
            ns: Vec<u8>,
            delegate_type: u8,
            req_id: Vec<u8>,
        ) -> DispatchResult {
            let owner = ensure_signed(origin)?;
            let dest = T::Lookup::lookup(delegate)?;
            if !<NsOwners<T>>::contains_key(&owner) {
                let bset: BoundedBTreeSet<
                    BoundedVec<u8, T::MaxBlockTransactions>,
                    T::MaxBlockTransactions,
                > = BoundedBTreeSet::new();
                <NsOwners<T>>::insert(owner.clone(), bset);
            }
            // add owner
            <NsOwners<T>>::mutate(&owner, |bset| {
                let ns_name: BoundedVec<_, _> = ns
                    .clone()
                    .try_into()
                    .map_err(|()| Error::<T>::TooManyTransactions)
                    .unwrap();
                if let Some(b) = bset {
                    if let Ok(_) = b.try_insert(ns_name) {}
                }
            });
            // add delegate
            if !<Delegates<T>>::contains_key(&dest) {
                let bmap: BoundedBTreeMap<
                    (BoundedVec<u8, T::MaxBlockTransactions>, T::AccountId),
                    u8,
                    T::MaxBlockTransactions,
                > = BoundedBTreeMap::new();
                <Delegates<T>>::insert(dest.clone(), bmap);
            }
            <Delegates<T>>::mutate(&dest, |bmap| {
                let ns_name: BoundedVec<_, _> = ns
                    .clone()
                    .try_into()
                    .map_err(|()| Error::<T>::TooManyTransactions)
                    .unwrap();
                if let Some(b) = bmap {
                    // TODO check delegate_type
                    if let Ok(_) = b.try_insert((ns_name, owner.clone()), delegate_type) {
                    }
                }
            });
            Self::deposit_event(
                Event::GeneralResultEvent{
                    status : 0,
                    msg : "ok".as_bytes().to_vec(),
                    req_id: req_id
            });
            Ok(())
        }

        #[pallet::weight(10000)]
        pub fn delete_delegate(
            origin: OriginFor<T>,
            delegate: AccountIdLookupOf<T>,
            ns: Vec<u8>,
            req_id: Vec<u8>,
        ) -> DispatchResult {
            let owner = ensure_signed(origin)?;
            let delegate_id = T::Lookup::lookup(delegate)?;
            if Self::is_ns_owner(owner.clone(), ns.clone()) {
                if <Delegates<T>>::contains_key(&delegate_id) {
                    <Delegates<T>>::mutate(&delegate_id, |bmap| {
                        let ns_name: BoundedVec<_, _> = ns
                            .clone()
                            .try_into()
                            .map_err(|()| Error::<T>::TooManyTransactions)
                            .unwrap();
                        if let Some(b) = bmap {
                            b.remove(&(ns_name, owner));
                        }
                    });
                    Self::deposit_event(
                        Event::GeneralResultEvent{
                            status : 0,
                            msg : "ok".as_bytes().to_vec(),
                            req_id: req_id
                        });
                } else {
                    Self::deposit_event(
                        Event::GeneralResultEvent{
                        status : 0,
                        msg : "fail to delete delegate. Delegate not exist".as_bytes().to_vec(),
                        req_id: req_id
                   }); 
                }
                
            } else {
                Self::deposit_event(
                    Event::GeneralResultEvent{
                    status : 0,
                    msg : "fail to delete delegate. Not the owner of ns".as_bytes().to_vec(),
                    req_id: req_id
               });
            }
            Ok(())
        }

        #[pallet::weight(10000)]
        pub fn add_delegate(
            origin: OriginFor<T>,
            delegate: AccountIdLookupOf<T>,
            ns: Vec<u8>,
            delegate_type: u8,
            req_id: Vec<u8>
        ) -> DispatchResult {
            let owner = ensure_signed(origin)?;
            let delegate_id = T::Lookup::lookup(delegate)?;
            if Self::is_ns_owner(owner.clone(), ns.clone()) {
                if !<Delegates<T>>::contains_key(&delegate_id) {
                    let bmap: BoundedBTreeMap<
                        (BoundedVec<u8, T::MaxBlockTransactions>, T::AccountId),
                        u8,
                        T::MaxBlockTransactions,
                    > = BoundedBTreeMap::new();
                    <Delegates<T>>::insert(delegate_id.clone(), bmap);
                }
                <Delegates<T>>::mutate(&delegate_id, |bmap| {
                    let ns_name: BoundedVec<_, _> = ns
                        .clone()
                        .try_into()
                        .map_err(|()| Error::<T>::TooManyTransactions)
                        .unwrap();
                    if let Some(b) = bmap {
                        // TODO check delegate_type
                        if let Ok(_) = b.try_insert((ns_name, owner.clone()), delegate_type) {
                        }
                    }
                });
                Self::deposit_event(
                    Event::GeneralResultEvent{
                        status : 0,
                        msg : "ok".as_bytes().to_vec(),
                        req_id: req_id
                    }
                );
            } else {
                Self::deposit_event(
                    Event::GeneralResultEvent{
                        status : 0,
                        msg : "fail to add delegate. Not the owner of ns".as_bytes().to_vec(),
                        req_id: req_id
                    }
                );
            }
            Ok(())
        }

        #[pallet::weight(10000)]
        pub fn run_sql_by_delegate(
            origin: OriginFor<T>,
            owner: AccountIdLookupOf<T>,
            data: Vec<u8>,
            req_id: Vec<u8>,
            ns: Vec<u8>,
        ) -> DispatchResult {
            let delegate = ensure_signed(origin)?;
            let owner_id = T::Lookup::lookup(owner)?;
            for (delegate_user, delegate_ns, delegate_type) in
                Self::list_delegates(owner_id.clone())
            {
                if delegate == delegate_user && delegate_ns == ns {
                    let bounded_sql: BoundedVec<_, _> = data
                        .clone()
                        .try_into()
                        .map_err(|()| Error::<T>::TooManyTransactions)?;
                    let bounded_id: BoundedVec<_, _> = req_id
                        .clone()
                        .try_into()
                        .map_err(|()| Error::<T>::TooManyTransactions)?;
                    let bounded_ns: BoundedVec<_, _> = ns
                        .clone()
                        .try_into()
                        .map_err(|()| Error::<T>::TooManyTransactions)?;
                    <PendingSQL<T>>::mutate(|sqls| {
                        if sqls.len() + 1 > T::MaxBlockTransactions::get() as usize {
                            return Err(Error::<T>::TooManyTransactions);
                        }
                        sqls.try_push((owner_id.clone(), bounded_sql, bounded_id, bounded_ns))
                            .map_err(|_| Error::<T>::TooManyTransactions)?;
                        Ok(())
                    })?;
                    Self::deposit_event(Event::SQLQueued(data));
                    return Ok(());
                }
            }
            Self::deposit_event(
                Event::GeneralResultEvent{
                    status : 0,
                    msg : "No NS Permission".as_bytes().to_vec(),
                    req_id: req_id
                });
            Ok(())
        }

        #[pallet::weight(10000)]
        pub fn run_sql_by_owner(
            origin: OriginFor<T>,
            data: Vec<u8>,
            req_id: Vec<u8>,
            ns: Vec<u8>,
        ) -> DispatchResult {
            let sender = ensure_signed(origin)?;
            if Self::is_ns_owner(sender.clone(), ns.clone()) {
                let bounded_sql: BoundedVec<_, _> = data
                    .clone()
                    .try_into()
                    .map_err(|()| Error::<T>::TooManyTransactions)?;
                let bounded_id: BoundedVec<_, _> = req_id
                    .clone()
                    .try_into()
                    .map_err(|()| Error::<T>::TooManyTransactions)?;
                let bounded_ns: BoundedVec<_, _> = ns
                    .clone()
                    .try_into()
                    .map_err(|()| Error::<T>::TooManyTransactions)?;
                <PendingSQL<T>>::mutate(|sqls| {
                    if sqls.len() + 1 > T::MaxBlockTransactions::get() as usize {
                        return Err(Error::<T>::TooManyTransactions);
                    }
                    sqls.try_push((sender.clone(), bounded_sql, bounded_id, bounded_ns))
                        .map_err(|_| Error::<T>::TooManyTransactions)?;
                    Ok(())
                })?;
                Self::deposit_event(Event::SQLQueued(data));
                Ok(())
            } else {
                Self::deposit_event(
                    Event::GeneralResultEvent{
                        status : 0,
                        msg : "No NS Permission".as_bytes().to_vec(),
                        req_id: req_id
                    });
                Ok(())
            }
        }

        /// Index and store data off chain. Minimum data size is 1 bytes, maximum is
        /// `MaxTransactionSize`. Data will be removed after `STORAGE_PERIOD` blocks, unless `renew`
        /// is called. # <weight>
        /// - n*log(n) of data size, as all data is pushed to an in-memory trie.
        /// Additionally contains a DB write.
        /// # </weight>
        #[pallet::weight(T::WeightInfo::store(data.len() as u32))]
        pub fn store(origin: OriginFor<T>, data: Vec<u8>) -> DispatchResult {
            ensure!(data.len() > 0, Error::<T>::EmptyTransaction);
            ensure!(
                data.len() <= T::MaxTransactionSize::get() as usize,
                Error::<T>::TransactionTooLarge
            );
            let sender = ensure_signed(origin)?;
            Self::apply_fee(sender, data.len() as u32)?;

            // Chunk data and compute storage root
            let chunk_count = num_chunks(data.len() as u32);
            let chunks = data.chunks(CHUNK_SIZE).map(|c| c.to_vec()).collect();
            let root = sp_io::trie::blake2_256_ordered_root(chunks, sp_runtime::StateVersion::V1);

            let content_hash = sp_io::hashing::blake2_256(&data);
            let extrinsic_index =
                <frame_system::Pallet<T>>::extrinsic_index().ok_or(Error::<T>::BadContext)?;
            sp_io::transaction_index::index(extrinsic_index, data.len() as u32, content_hash);

            let mut index = 0;
            <BlockTransactions<T>>::mutate(|transactions| {
                if transactions.len() + 1 > T::MaxBlockTransactions::get() as usize {
                    return Err(Error::<T>::TooManyTransactions);
                }
                let total_chunks = transactions.last().map_or(0, |t| t.block_chunks) + chunk_count;
                index = transactions.len() as u32;
                transactions
                    .try_push(TransactionInfo {
                        chunk_root: root,
                        size: data.len() as u32,
                        content_hash: content_hash.into(),
                        block_chunks: total_chunks,
                    })
                    .map_err(|_| Error::<T>::TooManyTransactions)?;
                Ok(())
            })?;
            Self::deposit_event(Event::Stored { index });
            Ok(())
        }

        /// Renew previously stored data. Parameters are the block number that contains
        /// previous `store` or `renew` call and transaction index within that block.
        /// Transaction index is emitted in the `Stored` or `Renewed` event.
        /// Applies same fees as `store`.
        /// # <weight>
        /// - Constant.
        /// # </weight>
        #[pallet::weight(T::WeightInfo::renew())]
        pub fn renew(
            origin: OriginFor<T>,
            block: T::BlockNumber,
            index: u32,
        ) -> DispatchResultWithPostInfo {
            let sender = ensure_signed(origin)?;
            let transactions = <Transactions<T>>::get(block).ok_or(Error::<T>::RenewedNotFound)?;
            let info = transactions
                .get(index as usize)
                .ok_or(Error::<T>::RenewedNotFound)?;
            let extrinsic_index =
                <frame_system::Pallet<T>>::extrinsic_index().ok_or(Error::<T>::BadContext)?;
            Self::apply_fee(sender, info.size)?;
            sp_io::transaction_index::renew(extrinsic_index, info.content_hash.into());

            let mut index = 0;
            <BlockTransactions<T>>::mutate(|transactions| {
                if transactions.len() + 1 > T::MaxBlockTransactions::get() as usize {
                    return Err(Error::<T>::TooManyTransactions);
                }
                let chunks = num_chunks(info.size);
                let total_chunks = transactions.last().map_or(0, |t| t.block_chunks) + chunks;
                index = transactions.len() as u32;
                transactions
                    .try_push(TransactionInfo {
                        chunk_root: info.chunk_root,
                        size: info.size,
                        content_hash: info.content_hash,
                        block_chunks: total_chunks,
                    })
                    .map_err(|_| Error::<T>::TooManyTransactions)
            })?;
            Self::deposit_event(Event::Renewed { index });
            Ok(().into())
        }

        /// Check storage proof for block number `block_number() - StoragePeriod`.
        /// If such block does not exist the proof is expected to be `None`.
        /// # <weight>
        /// - Linear w.r.t the number of indexed transactions in the proved block for random
        ///   probing.
        /// There's a DB read for each transaction.
        /// Here we assume a maximum of 100 probed transactions.
        /// # </weight>
        #[pallet::weight((T::WeightInfo::check_proof_max(), DispatchClass::Mandatory))]
        pub fn check_proof(
            origin: OriginFor<T>,
            proof: TransactionStorageProof,
        ) -> DispatchResultWithPostInfo {
            ensure_none(origin)?;
            ensure!(!ProofChecked::<T>::get(), Error::<T>::DoubleCheck);
            let number = <frame_system::Pallet<T>>::block_number();
            let period = <StoragePeriod<T>>::get();
            let target_number = number.saturating_sub(period);
            ensure!(!target_number.is_zero(), Error::<T>::UnexpectedProof);
            let total_chunks = <ChunkCount<T>>::get(target_number);
            ensure!(total_chunks != 0, Error::<T>::UnexpectedProof);
            let parent_hash = <frame_system::Pallet<T>>::parent_hash();
            let selected_chunk_index = random_chunk(parent_hash.as_ref(), total_chunks);
            let (info, chunk_index) = match <Transactions<T>>::get(target_number) {
                Some(infos) => {
                    let index = match infos
                        .binary_search_by_key(&selected_chunk_index, |info| info.block_chunks)
                    {
                        Ok(index) => index,
                        Err(index) => index,
                    };
                    let info = infos
                        .get(index)
                        .ok_or(Error::<T>::MissingStateData)?
                        .clone();
                    let chunks = num_chunks(info.size);
                    let prev_chunks = info.block_chunks - chunks;
                    (info, selected_chunk_index - prev_chunks)
                }
                None => return Err(Error::<T>::MissingStateData.into()),
            };
            ensure!(
                sp_io::trie::blake2_256_verify_proof(
                    info.chunk_root,
                    &proof.proof,
                    &encode_index(chunk_index),
                    &proof.chunk,
                    sp_runtime::StateVersion::V1,
                ),
                Error::<T>::InvalidProof
            );
            ProofChecked::<T>::put(true);
            Self::deposit_event(Event::ProofChecked);
            Ok(().into())
        }
    }

    #[pallet::event]
    #[pallet::generate_deposit(pub(super) fn deposit_event)]
    pub enum Event<T: Config> {
        /// Stored data under specified index.
        Stored {
            index: u32,
        },
        /// Renewed data under specified index.
        Renewed {
            index: u32,
        },
        /// Storage proof was successfully checked.
        ProofChecked,
        SQLQueued(Vec<u8>),
        SQLResult(Vec<u8>),
        AddDelegateOK(Vec<u8>),
        DeleteDelegateOk(Vec<u8>),

        /// General result event 
        GeneralResultEvent {
            status: u32,
            msg: Vec<u8>,
            req_id: Vec<u8>
        },
        NoNsPermission,
    }


    /// Collection of transaction metadata by block number.
    #[pallet::storage]
    #[pallet::getter(fn transaction_roots)]
    pub(super) type Transactions<T: Config> = StorageMap<
        _,
        Blake2_128Concat,
        T::BlockNumber,
        BoundedVec<TransactionInfo, T::MaxBlockTransactions>,
        OptionQuery,
    >;

    /// Count indexed chunks for each block.
    #[pallet::storage]
    pub(super) type ChunkCount<T: Config> =
        StorageMap<_, Blake2_128Concat, T::BlockNumber, u32, ValueQuery>;

    #[pallet::storage]
    #[pallet::getter(fn table_owners)]
    pub type NsOwners<T: Config> = StorageMap<
        _,
        Twox64Concat,
        T::AccountId,
        BoundedBTreeSet<BoundedVec<u8, T::MaxBlockTransactions>, T::MaxBlockTransactions>,
    >;

    #[pallet::storage]
    pub type Delegates<T: Config> = StorageMap<
        _,
        Twox64Concat,
        T::AccountId,
        BoundedBTreeMap<
            (BoundedVec<u8, T::MaxBlockTransactions>, T::AccountId),
            u8,
            T::MaxBlockTransactions,
        >,
    >;

    #[pallet::storage]
    #[pallet::getter(fn byte_fee)]
    /// Storage fee per byte.
    pub(super) type ByteFee<T: Config> = StorageValue<_, BalanceOf<T>>;

    #[pallet::storage]
    #[pallet::getter(fn entry_fee)]
    /// Storage fee per transaction.
    pub(super) type EntryFee<T: Config> = StorageValue<_, BalanceOf<T>>;

    /// Storage period for data in blocks. Should match `sp_storage_proof::DEFAULT_STORAGE_PERIOD`
    /// for block authoring.
    #[pallet::storage]
    pub(super) type StoragePeriod<T: Config> = StorageValue<_, T::BlockNumber, ValueQuery>;

    // Intermediates
    #[pallet::storage]
    pub(super) type BlockTransactions<T: Config> =
        StorageValue<_, BoundedVec<TransactionInfo, T::MaxBlockTransactions>, ValueQuery>;

    /// Was the proof checked in this block?
    #[pallet::storage]
    pub(super) type ProofChecked<T: Config> = StorageValue<_, bool, ValueQuery>;

    #[pallet::storage]
    pub(super) type PendingSQL<T: Config> = StorageValue<
        _,
        BoundedVec<
            (
                T::AccountId,
                BoundedVec<u8, T::MaxBlockTransactions>,
                BoundedVec<u8, T::MaxBlockTransactions>,
                BoundedVec<u8, T::MaxBlockTransactions>,
            ),
            T::MaxBlockTransactions,
        >,
        ValueQuery,
    >;

    #[pallet::genesis_config]
    pub struct GenesisConfig<T: Config> {
        pub byte_fee: BalanceOf<T>,
        pub entry_fee: BalanceOf<T>,
        pub storage_period: T::BlockNumber,
    }

    #[cfg(feature = "std")]
    impl<T: Config> Default for GenesisConfig<T> {
        fn default() -> Self {
            Self {
                byte_fee: 10u32.into(),
                entry_fee: 1000u32.into(),
                storage_period: sp_transaction_storage_proof::DEFAULT_STORAGE_PERIOD.into(),
            }
        }
    }

    #[pallet::genesis_build]
    impl<T: Config> GenesisBuild<T> for GenesisConfig<T> {
        fn build(&self) {
            <ByteFee<T>>::put(&self.byte_fee);
            <EntryFee<T>>::put(&self.entry_fee);
            <StoragePeriod<T>>::put(&self.storage_period);
        }
    }

    #[pallet::inherent]
    impl<T: Config> ProvideInherent for Pallet<T> {
        type Call = Call<T>;
        type Error = InherentError;
        const INHERENT_IDENTIFIER: InherentIdentifier = INHERENT_IDENTIFIER;

        fn create_inherent(data: &InherentData) -> Option<Self::Call> {
            let proof = data
                .get_data::<TransactionStorageProof>(&Self::INHERENT_IDENTIFIER)
                .unwrap_or(None);
            proof.map(|proof| Call::check_proof { proof })
        }

        fn check_inherent(
            _call: &Self::Call,
            _data: &InherentData,
        ) -> result::Result<(), Self::Error> {
            Ok(())
        }

        fn is_inherent(call: &Self::Call) -> bool {
            matches!(call, Call::check_proof { .. })
        }
    }

    impl<T: Config> Pallet<T> {
        fn apply_fee(sender: T::AccountId, size: u32) -> DispatchResult {
            let byte_fee = ByteFee::<T>::get().ok_or(Error::<T>::NotConfigured)?;
            let entry_fee = EntryFee::<T>::get().ok_or(Error::<T>::NotConfigured)?;
            let fee = byte_fee
                .saturating_mul(size.into())
                .saturating_add(entry_fee);
            ensure!(
                T::Currency::can_slash(&sender, fee),
                Error::<T>::InsufficientFunds
            );
            let (credit, _) = T::Currency::slash(&sender, fee);
            T::FeeDestination::on_unbalanced(credit);
            Ok(())
        }

        pub fn is_ns_owner(owner: T::AccountId, ns: Vec<u8>) -> bool {
            if let Ok(v) = NsOwners::<T>::try_get(&owner) {
                let bns: BoundedVec<_, _> = ns
                    .clone()
                    .try_into()
                    .map_err(|()| Error::<T>::TooManyTransactions)
                    .unwrap();
                v.into_inner().contains(&bns)
            } else {
                false
            }
        }

        pub fn list_delegates(owner: T::AccountId) -> Vec<(T::AccountId, Vec<u8>, u8)> {
            let mut result: Vec<(T::AccountId, Vec<u8>, u8)> = Vec::new();
            for (delegate, tree_map) in Delegates::<T>::iter() {
                for ((ns, o), delegate_type) in tree_map.into_iter() {
                    if owner == o {
                        result.push((delegate.clone(), ns.into(), delegate_type));
                    }
                }
            }
            result
        }

        fn run_remote_sql(
            query: &str,
            account: &str,
            req_id: &str,
        ) -> Result<Vec<u8>, http::Error> {
            let deadline = sp_io::offchain::timestamp().add(Duration::from_millis(2_000));
            let sql_input = SQLInput {
                query,
                account,
                req_id,
            };
            let data = serde_json::to_vec(&sql_input).unwrap();
            let request = http::Request::default()
                .add_header("Content-Type", "application/json")
                .method(http::Method::Post)
                .url("http://localhost:8080/query")
                .body(vec![data]);
            let pending = request.send().map_err(|_| http::Error::IoError)?;
            let response = pending
                .try_wait(deadline)
                .map_err(|_| http::Error::DeadlineReached)??;
            if response.code != 200 {
                log::warn!("Unexpected status code: {}", response.code);
                Err(http::Error::Unknown)
            } else {
                let body = response.body().collect::<Vec<u8>>();
                log::info!("get result {:?} ok", body);
                Ok(body)
            }
        }

        fn run_and_send_signed_payload(
            query: &str,
            account: &str,
            req_id: &str,
        ) -> Result<(), &'static str> {
            let data =
                Self::run_remote_sql(query, account, req_id).map_err(|_| "Failed to run sql")?;
            log::info!("process sql {} successully", query);
            let signer = Signer::<T, T::AuthorityId>::all_accounts();
            if !signer.can_sign() {
                return Err(
                    "No local accounts available. Consider adding one via `author_insertKey` RPC.",
                );
            }
            signer.send_signed_transaction(|_account| {
                // Received price is wrapped into a call to `submit_price` public function of this
                // pallet. This means that the transaction, when executed, will simply call that
                // function passing `price` as an argument.
                Call::submit_result {
                    data: InputPayload { data: data.clone() },
                }
            });
            Ok(())
        }
    }
}
