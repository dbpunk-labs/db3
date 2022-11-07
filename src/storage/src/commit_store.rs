//
// commit_store.rs
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

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use db3_error::{DB3Error, Result};
use merk::{Merk, Op};
use std::pin::Pin;
const COMMIT_KEY: &str = "_DB3_COMMIT_KEY_";

pub struct CommitStore {}
impl CommitStore {
    pub fn new() -> Self {
        Self {}
    }

    pub fn apply(db: Pin<&mut Merk>, height: u64) -> Result<()> {
        let key = COMMIT_KEY.as_bytes().to_vec();
        let mut value: Vec<u8> = Vec::new();
        value
            .write_u64::<BigEndian>(height)
            .map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        let entries = vec![(key, Op::Put(value))];
        unsafe {
            Pin::get_unchecked_mut(db)
                .apply(&entries, &[])
                .map_err(|e| DB3Error::ApplyCommitError(format!("{}", e)))?;
        }
        Ok(())
    }

    pub fn get_applied_height(db: Pin<&Merk>) -> Result<Option<u64>> {
        let key = COMMIT_KEY.as_bytes().to_vec();
        let value = db
            .get(key.as_ref())
            .map_err(|e| DB3Error::GetCommitError(format!("{}", e)))?;
        if let Some(v) = value {
            let ref_value: &[u8] = v.as_ref();
            let height = (&ref_value[0..])
                .read_u64::<BigEndian>()
                .map_err(|e| DB3Error::GetCommitError(format!("{}", e)))?;
            Ok(Some(height))
        } else {
            // for the first time
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    #[test]
    fn it_works() {
        let tmp_dir_path = TempDir::new("commit_store").expect("create temp dir");
        let merk = Merk::open(tmp_dir_path).unwrap();
        let mut db = Box::pin(merk);
        let result = CommitStore::get_applied_height(db.as_ref());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
        let db_m: Pin<&mut Merk> = Pin::as_mut(&mut db);
        let result = CommitStore::apply(db_m, 1);
        assert!(result.is_ok());
        let result = CommitStore::get_applied_height(db.as_ref());
        if let Ok(Some(v)) = result {
            assert_eq!(1, v);
        } else {
            assert!(false);
        }
    }
}
