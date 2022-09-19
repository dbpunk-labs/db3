//
// lib.rs
// Copyright (C) 2022 parallels <parallels@parallels-Parallels-Virtual-Platform>
// Distributed under terms of the MIT license.
//

#![cfg_attr(not(feature = "std"), no_std)]
use codec::Codec;
use sp_std::vec::Vec;
sp_api::decl_runtime_apis! {
    /// The API to interact with contracts without using executive.
    pub trait DBAccountApi<AccountId> where
        AccountId: Codec,
    {
        /// Perform a call from a specified account to a given contract.
        ///
        /// See `pallet_sqldb::Pallet::call`.
        fn is_ns_owner(
            origin: AccountId,
            ns: Vec<u8>,
        ) -> bool;

        fn list_delegates(origin: AccountId) -> Vec<(AccountId, Vec<u8>, u8)>;
    }
}
