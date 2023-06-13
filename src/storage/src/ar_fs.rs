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
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use tracing::info;

#[derive(Clone)]
pub struct ArFileSystemConfig {
    pub wallet_path: String,
    pub arweave_url: String,
}

pub struct ArFileSystem {
    arweave: Arweave,
    wallet: WalletInfoClient,
}

impl ArFileSystem {
    pub fn new(config: ArFileSystemConfig) -> Result<Self> {
        let arweave_url = url::Url::from_str(config.arweave_url.as_str())
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let path = Path::new(config.wallet_path.as_str());
        let arweave = Arweave::from_keypair_path(&path, arweave_url.clone())
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let addr = arweave.get_wallet_address();
        info!(
            "new ar filestore with url {} and adr {}",
            config.arweave_url.as_str(),
            addr.as_str()
        );
        let wallet = WalletInfoClient::new(arweave_url);
        Ok(Self { arweave, wallet })
    }

    pub fn get_address(&self) -> String {
        self.arweave.get_wallet_address()
    }

    pub async fn get_balance(&self) -> Result<Currency> {
        let balance = self
            .wallet
            .balance(self.arweave.get_wallet_address().as_str())
            .await
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let currency = Currency::from_str(balance.as_str())
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        Ok(currency)
    }

    pub async fn download_file(&self, path_to_write: &Path, tx: &str) -> Result<()> {
        let tx_b64 = Base64::from_str(tx).map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let (_status, data) = self
            .arweave
            .get_tx_data(&tx_b64)
            .await
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        if let Some(d) = data {
            let mut f =
                File::create(path_to_write).map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            f.write_all(d.as_ref())
                .map_err(|e| DB3Error::RollupError(format!("{e}")))?;

            f.sync_all()
                .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            Ok(())
        } else {
            Err(DB3Error::RollupError("fail to download file".to_string()))
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
                .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            let block_start_tag: Tag<Base64> =
                Tag::from_utf8_strs("Start-Block", start_block.to_string().as_str())
                    .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            let block_end_tag: Tag<Base64> =
                Tag::from_utf8_strs("End-Block", end_block.to_string().as_str())
                    .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            let filename_tag: Tag<Base64> = Tag::from_utf8_strs("File-Name", filename)
                .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            let network_tag: Tag<Base64> =
                Tag::from_utf8_strs("Network-Id", network_id.to_string().as_str())
                    .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            vec![
                app_tag,
                block_start_tag,
                block_end_tag,
                filename_tag,
                network_tag,
            ]
        };

        if !last_ar_tx.is_empty() {
            let value =
                Base64::from_str(last_ar_tx).map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            let name = Base64::from_utf8_str("Last-Rollup-Tx")
                .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
            let last_rollup_tx = Tag::<Base64> { value, name };
            tags.push(last_rollup_tx);
        }

        let metadata =
            std::fs::metadata(path).map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let fee = self
            .arweave
            .get_fee_by_size(metadata.len())
            .await
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        self.arweave
            .upload_file_from_path(path, tags, fee)
            .await
            .map_err(|e| DB3Error::RollupError(format!("{e}")))
    }

    pub async fn get_tx_status(&self, id: &str) -> Result<Option<TxStatus>> {
        let tx_id = Base64::from_str(id).map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        let (code, status) = self
            .arweave
            .get_tx_status(&tx_id)
            .await
            .map_err(|e| DB3Error::RollupError(format!("{e}")))?;
        if code == StatusCode::ACCEPTED {
            Ok(None)
        } else if code == StatusCode::OK {
            Ok(status)
        } else {
            Err(DB3Error::RollupError("fail to get tx status ".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {}
