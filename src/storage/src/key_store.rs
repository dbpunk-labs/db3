//
// key_store.rs
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

use db3_error::{DB3Error, Result};
use fastcrypto::encoding::Base64;
use fastcrypto::encoding::Encoding;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

pub struct KeyStoreConfig {
    pub key_root_path: String,
}

pub struct KeyStore {
    config: KeyStoreConfig,
}

impl KeyStore {
    pub fn new(config: KeyStoreConfig) -> Self {
        Self { config }
    }

    //
    // check if the key exist with path key_root_path + '/' + key.secret
    //
    pub fn has_key(&self, key: &str) -> bool {
        let key_filename = format!("{key}.secret");
        let mut path_buf = PathBuf::new();
        path_buf.push(self.config.key_root_path.as_str());
        path_buf.push(key_filename.as_str());
        let path = path_buf.as_path();
        path.exists()
    }

    pub fn write_key(&self, key: &str, secret: &[u8]) -> Result<()> {
        if self.has_key(key) {
            return Err(DB3Error::WriteStoreError("key exist".to_string()));
        }
        let key_filename = format!("{key}.secret");
        let mut path_buf = PathBuf::new();
        path_buf.push(self.config.key_root_path.as_str());
        path_buf.push(key_filename.as_str());
        let path = path_buf.as_path();
        let b64_str = Base64::encode(secret);
        let mut f = File::create(path).map_err(|e| {
            DB3Error::WriteStoreError(format!(
                "keystore fail to open file {e} with path {:?}",
                path
            ))
        })?;
        f.write_all(b64_str.as_bytes())
            .map_err(|e| DB3Error::WriteStoreError(format!("keystore fail to open file {e}")))?;
        f.sync_all()
            .map_err(|e| DB3Error::WriteStoreError(format!("fail to open file {e}")))?;
        Ok(())
    }

    pub fn get_key(&self, key: &str) -> Result<Vec<u8>> {
        if !self.has_key(key) {
            return Err(DB3Error::ReadStoreError("key exist".to_string()));
        }
        let key_filename = format!("{key}.secret");
        let mut path_buf = PathBuf::new();
        path_buf.push(self.config.key_root_path.as_str());
        path_buf.push(key_filename.as_str());
        let path = path_buf.as_path();
        let data = std::fs::read(path)
            .map_err(|e| DB3Error::ReadStoreError(format!("fail to open file {e}")))?;
        let b64_str = std::str::from_utf8(data.as_ref()).map_err(|e| {
            DB3Error::ReadStoreError(format!("decode content with key {key}  with error {e}"))
        })?;
        let bytes = Base64::decode(b64_str).map_err(|e| {
            DB3Error::ReadStoreError(format!(
                "fail to b64 decode with {key}, {b64_str} and error {e}"
            ))
        })?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ethers::prelude::{LocalWallet, Signer};
    use std::ops::Deref;
    use std::fs;
    use tempdir::TempDir;

    #[test]
    fn test_has_key_existing() {
        let tmp_dir_path = TempDir::new("tmp_dir").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let keystore = KeyStore::new(KeyStoreConfig { key_root_path: real_path.clone() });
        let key = "test_key";
        let value = b"value";
        keystore.write_key(key, value).unwrap();
        assert_eq!(keystore.has_key(key), true);
    }

    #[test]
    fn test_has_key_non_existing() {
        let tmp_dir_path = TempDir::new("tmp_dir").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let keystore = KeyStore::new(KeyStoreConfig { key_root_path: real_path.clone() });
        let key = "test_key";
        assert_eq!(keystore.has_key(key), false);
    }

    #[test]
    fn test_has_key_invalid_input() {
        let tmp_dir_path = TempDir::new("tmp_dir").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let keystore = KeyStore::new(KeyStoreConfig { key_root_path: real_path.clone() });
        let empty_key = "";
        assert_eq!(keystore.has_key(empty_key), false);
    }

    #[test]
    fn test_has_key_file_error() {
        let tmp_dir_path = TempDir::new("tmp_dir").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let keystore = KeyStore::new(KeyStoreConfig { key_root_path: real_path.clone() });
        let mut invalid_file = std::fs::File::create(format!("{}/INVALID", real_path)).unwrap();
        invalid_file.write_all(b"invalid content").unwrap(); // create an invalid file
        let invalid_key = "INVALID";
        assert_eq!(keystore.has_key(invalid_key), false);
    }


    #[test]
    fn evm_account_key_store_smoke_test() {
        let tmp_dir_path = TempDir::new("key store").expect("create temp dir");
        let real_path = tmp_dir_path.path().to_str().unwrap().to_string();
        let config = KeyStoreConfig {
            key_root_path: real_path,
        };
        let mut rng = rand::thread_rng();
        let wallet = LocalWallet::new(&mut rng);
        let data = wallet.signer().to_bytes();
        let key_store = KeyStore::new(config);
        let result = key_store.write_key("evm_account", data.deref());
        assert!(result.is_ok());
        if let Ok(d) = key_store.get_key("evm_account") {
            assert_eq!(&d, data.deref());
            let wallet2 = LocalWallet::from_bytes(&d).unwrap();
            assert_eq!(wallet.address(), wallet2.address());
        }
    }
}
