//
// ar_fs.rs
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

use crate::key_store::{KeyStore, KeyStoreConfig};
use arweave_rs::crypto::base64::Base64;
use arweave_rs::currency::Currency;
use arweave_rs::{
    transaction::tags::{FromUtf8Strs, Tag},
    types::TxStatus,
    wallet::WalletInfoClient,
    Arweave,
};
use db3_error::{DB3Error, Result};
use http::StatusCode;
use rsa::{pkcs8::DecodePrivateKey, pkcs8::EncodePrivateKey, RsaPrivateKey};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use tracing::info;

#[derive(Clone)]
pub struct ArFileSystemConfig {
    pub arweave_url: String,
    pub key_root_path: String,
}

pub struct ArFileSystem {
    arweave: Arweave,
    wallet: WalletInfoClient,
}

impl ArFileSystem {
    pub fn new(config: ArFileSystemConfig) -> Result<Self> {
        let arweave =
            Self::build_arweave(config.key_root_path.as_str(), config.arweave_url.as_str())?;
        let addr = arweave.get_wallet_address();
        info!(
            "new ar filestore with url {} and addr {}",
            config.arweave_url.as_str(),
            addr.as_str()
        );
        let arweave_url = url::Url::from_str(config.arweave_url.as_str())
            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;

        let wallet = WalletInfoClient::new(arweave_url);
        Ok(Self { arweave, wallet })
    }

    fn build_arweave(key_root_path: &str, url: &str) -> Result<Arweave> {
        let arweave_url =
            url::Url::from_str(url).map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        info!("recover ar key store from {}", key_root_path);
        let key_store_config = KeyStoreConfig {
            key_root_path: key_root_path.to_string(),
        };
        let key_store = KeyStore::new(key_store_config);
        match key_store.has_key("ar") {
            true => {
                let data = key_store.get_key("ar")?;
                let data_ref: &[u8] = &data;
                let priv_key: RsaPrivateKey = RsaPrivateKey::from_pkcs8_der(data_ref)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                Arweave::from_private_key(priv_key, arweave_url)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))
            }
            false => {
                let mut rng = rand::thread_rng();
                let bits = 2048;
                let priv_key = RsaPrivateKey::new(&mut rng, bits)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                let doc = priv_key
                    .to_pkcs8_der()
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                key_store
                    .write_key("ar", doc.as_ref())
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
                Arweave::from_private_key(priv_key, arweave_url)
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))
            }
        }
    }

    pub fn get_address(&self) -> String {
        self.arweave.get_wallet_address()
    }

    pub async fn get_balance(&self) -> Result<Currency> {
        let balance = self
            .wallet
            .balance(self.arweave.get_wallet_address().as_str())
            .await
            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        let currency = Currency::from_str(balance.as_str())
            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        Ok(currency)
    }

    pub async fn download_file(&self, path_to_write: &Path, tx: &str) -> Result<()> {
        let tx_b64 = Base64::from_str(tx).map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        let (_status, data) = self
            .arweave
            .get_tx_data(&tx_b64)
            .await
            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        if let Some(d) = data {
            let mut f =
                File::create(path_to_write).map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
            f.write_all(d.as_ref())
                .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;

            f.sync_all()
                .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
            Ok(())
        } else {
            Err(DB3Error::ArwareOpError("fail to download file".to_string()))
        }
    }

    pub async fn upload_file(
        &self,
        path: &Path,
        last_ar_tx: &str,
        start_block: u64,
        end_block: u64,
        network_id: u64,
        filename: &str,
    ) -> Result<(String, u64)> {
        let mut tags: Vec<Tag<Base64>> = {
            let app_tag: Tag<Base64> = Tag::from_utf8_strs("App-Name", "DB3 Network")
                .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
            let block_start_tag: Tag<Base64> =
                Tag::from_utf8_strs("Start-Block", start_block.to_string().as_str())
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
            let block_end_tag: Tag<Base64> =
                Tag::from_utf8_strs("End-Block", end_block.to_string().as_str())
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
            let filename_tag: Tag<Base64> = Tag::from_utf8_strs("File-Name", filename)
                .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
            let network_tag: Tag<Base64> =
                Tag::from_utf8_strs("Network-Id", network_id.to_string().as_str())
                    .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
            vec![
                app_tag,
                block_start_tag,
                block_end_tag,
                filename_tag,
                network_tag,
            ]
        };

        if !last_ar_tx.is_empty() {
            let value = Base64::from_utf8_str(last_ar_tx)
                .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
            let name = Base64::from_utf8_str("Last-Rollup-Tx")
                .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
            let last_rollup_tx = Tag::<Base64> { value, name };
            tags.push(last_rollup_tx);
        }

        let metadata =
            std::fs::metadata(path).map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        let fee = self
            .arweave
            .get_fee_by_size(metadata.len())
            .await
            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        self.arweave
            .upload_file_from_path(path, tags, fee)
            .await
            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))
    }

    pub async fn get_tx_status(&self, id: &str) -> Result<Option<TxStatus>> {
        let tx_id = Base64::from_str(id).map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        let (code, status) = self
            .arweave
            .get_tx_status(&tx_id)
            .await
            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        if code == StatusCode::ACCEPTED {
            Ok(None)
        } else if code == StatusCode::OK {
            Ok(status)
        } else {
            Err(DB3Error::ArwareOpError(
                "fail to get tx status ".to_string(),
            ))
        }
    }
    pub async fn get_tags(&self, id_str: &str) -> Result<Vec<Tag<Base64>>> {
        let tx_id =
            Base64::from_str(id_str).map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;
        let (_status, tx) = self
            .arweave
            .get_tx(&tx_id)
            .await
            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?;

        if let Some(t) = tx {
            Ok(t.tags)
        } else {
            Err(DB3Error::ArwareOpError("fail to get tx tags ".to_string()))
        }
    }

    /// get last rollup tag
    pub async fn get_last_rollup_tag(&self, id_str: &str) -> Result<Option<String>> {
        let tags = self.get_tags(id_str).await?;
        for tag in tags {
            if let Ok(name) = tag.name.to_utf8_string() {
                if name == "Last-Rollup-Tx" {
                    return Ok(Some(
                        tag.value
                            .to_utf8_string()
                            .map_err(|e| DB3Error::ArwareOpError(format!("{e}")))?,
                    ));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {}
