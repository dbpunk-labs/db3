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
use db3_error::{DB3Error, Result};
use db3_proto::db3_account_proto::Account;
use db3_proto::db3_base_proto::{UnitType, Units};
use db3_types::account_key::AccountKey;
use ethereum_types::Address as AccountAddress;
use merk::{Merk, Op};
use prost::Message;
use std::pin::Pin;

pub struct AccountStore {}

impl AccountStore {
    pub fn new() -> Self {
        Self {}
    }

    pub fn apply(
        db: Pin<&mut Merk>,
        account_addr: &AccountAddress,
        account: &Account,
    ) -> Result<()> {
        let key = AccountKey(*account_addr);
        let encoded_key = key.encode()?;
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

    pub fn get_account(db: Pin<&Merk>, account_addr: &AccountAddress) -> Result<Account> {
        let key = AccountKey(*account_addr);
        let encoded_key = key.encode()?;
        //TODO verify the result
        let values = db
            .get(encoded_key.as_ref())
            .map_err(|e| DB3Error::GetAccountError(format!("{}", e)))?;
        if let Some(v) = values {
            match Account::decode(v.as_ref()) {
                Ok(a) => Ok(a),
                Err(e) => Err(DB3Error::GetAccountError(format!("{}", e))),
            }
        } else {
            //TODO assign 10 db3 credits
            Ok(Account {
                total_bills: Some(Units {
                    utype: UnitType::Tai.into(),
                    amount: 0,
                }),
                total_storage_in_bytes: 0,
                total_mutation_count: 0,
                total_query_session_count: 0,
                credits: Some(Units {
                    utype: UnitType::Db3.into(),
                    amount: 10,
                }),
                nonce: 0,
                bill_next_id: 0,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_base::get_a_static_address;
    use std::boxed::Box;
    use tempdir::TempDir;
    #[test]
    fn it_apply_account() {
        let tmp_dir_path = TempDir::new("apply_account").expect("create temp dir");
        let addr = get_a_static_address();
        let mut merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let account = Account {
            total_bills: Some(Units {
                utype: UnitType::Db3.into(),
                amount: 2,
            }),
            total_storage_in_bytes: 10,
            total_mutation_count: 10,
            total_query_session_count: 5,
            credits: Some(Units {
                utype: UnitType::Db3.into(),
                amount: 10,
            }),
            nonce: 10,
            bill_next_id: 10,
        };
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = AccountStore::apply(db_m, &addr, &account);
        assert!(result.is_ok());
        let account_ret = AccountStore::get_account(db.as_ref(), &addr);
        assert!(account_ret.is_ok());
        if let Ok(a) = account_ret {
            assert_eq!(a.total_bills, account.total_bills);
        } else {
            assert!(false);
        }
    }
}
