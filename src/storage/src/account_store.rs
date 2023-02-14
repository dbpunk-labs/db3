//
// account_store.rs
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
//
use bytes::BytesMut;
use db3_crypto::db3_address::DB3Address;
use db3_error::{DB3Error, Result};
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_base_proto::{UnitType, Units};
use db3_types::{account_key::AccountKey, gas};
use merkdb::{Merk, Op};
use prost::Message;
use std::pin::Pin;

pub struct AccountStore {}

impl AccountStore {
    pub fn new() -> Self {
        Self {}
    }

    ///
    /// override the account with a new one
    ///
    fn override_account(db: Pin<&mut Merk>, encoded_key: Vec<u8>, account: &Account) -> Result<()> {
        let mut buf = BytesMut::with_capacity(1024);
        account
            .encode(&mut buf)
            .map_err(|e| DB3Error::ApplyAccountError(format!("{}", e)))?;
        let buf = buf.freeze();
        let entry = (encoded_key, Op::Put(buf.to_vec()));
        unsafe {
            Pin::get_unchecked_mut(db)
                .apply(&[entry], &[])
                .map_err(|e| DB3Error::ApplyAccountError(format!("{}", e)))?;
        }
        Ok(())
    }

    ///
    /// update the account
    ///
    pub fn update_account(db: Pin<&mut Merk>, addr: &DB3Address, account: &Account) -> Result<()> {
        let key = AccountKey(addr);
        let encoded_key = key.encode()?;
        Self::override_account(db, encoded_key, account)
    }

    ///
    /// Create a account for the storage chains
    ///
    ///
    pub fn new_account(db: Pin<&mut Merk>, addr: &DB3Address) -> Result<Account> {
        let key = AccountKey(addr);
        let encoded_key = key.encode()?;
        let values = db
            .get(encoded_key.as_ref())
            .map_err(|e| DB3Error::GetAccountError(format!("{}", e)))?;
        if let Some(v) = values {
            match Account::decode(v.as_ref()) {
                Ok(a) => Ok(a),
                Err(e) => Err(DB3Error::GetAccountError(format!("{}", e))),
            }
        } else {
            let new_account = Account {
                bills: Some(Units {
                    utype: UnitType::Tai.into(),
                    amount: 0,
                }),
                credits: Some(Units {
                    utype: UnitType::Db3.into(),
                    amount: 10,
                }),
                total_storage_in_bytes: 0,
                total_mutation_count: 0,
                total_session_count: 0,
                nonce: 0,
            };
            Self::override_account(db, encoded_key, &new_account)?;
            Ok(new_account)
        }
    }

    fn get_account_internal(db: Pin<&Merk>, key: &[u8]) -> Result<Option<Account>> {
        let values = db
            .get(key)
            .map_err(|e| DB3Error::GetAccountError(format!("{}", e)))?;
        if let Some(v) = values {
            match Account::decode(v.as_ref()) {
                Ok(a) => Ok(Some(a)),
                Err(e) => Err(DB3Error::GetAccountError(format!("{}", e))),
            }
        } else {
            Ok(None)
        }
    }

    ///
    /// get account from account store
    ///
    ///
    pub fn get_account(db: Pin<&Merk>, addr: &DB3Address) -> Result<Option<Account>> {
        let key = AccountKey(addr);
        let encoded_key = key.encode()?;
        Self::get_account_internal(db, encoded_key.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_crypto::key_derive;
    use db3_crypto::signature_scheme::SignatureScheme;
    use std::boxed::Box;
    use tempdir::TempDir;

    fn gen_address() -> DB3Address {
        let seed: [u8; 32] = [0; 32];
        let (address, _) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::ED25519).unwrap();
        address
    }

    #[test]
    fn it_account_smoke_test() {
        let tmp_dir_path = TempDir::new("apply_account").expect("create temp dir");
        let addr = gen_address();
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let account = Account {
            bills: Some(Units {
                utype: UnitType::Db3.into(),
                amount: 2,
            }),
            credits: Some(Units {
                utype: UnitType::Db3.into(),
                amount: 10,
            }),
            total_storage_in_bytes: 10,
            total_mutation_count: 10,
            total_session_count: 5,
            nonce: 10,
        };
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = AccountStore::new_account(db_m, &addr);
        assert!(result.is_ok());
        let account_opt = AccountStore::get_account(db.as_ref(), &addr);
        assert!(account_opt.is_ok());
        assert!(account_opt.unwrap().is_some());
    }
}
