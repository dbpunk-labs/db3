//
// keystore.rs
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

use bip32::Mnemonic;
use db3_crypto::{
    db3_address::DB3Address,
    db3_keypair::{DB3KeyPair, EncodeDecodeBase64},
    id::AccountId,
    key_derive,
    signature_scheme::SignatureScheme,
};
use db3_error::Result;
use dirs;
use ethers::signers::LocalWallet;
use ethers::signers::Signer;
use fastcrypto::encoding::{Base64, Encoding};
use hex;
use rand_core::OsRng;
use std::fs::File;
use std::io::Write;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::str::FromStr;

use prettytable::{format, Table};

pub struct KeyStore {
    key_pair: DB3KeyPair,
}

impl KeyStore {
    pub fn new(key_pair: DB3KeyPair) -> Self {
        Self { key_pair }
    }

    //
    // generate the keypair for a new user
    //
    //
    pub fn generate_keypair() -> Result<(AccountId, DB3KeyPair, String)> {
        let mnemonic = Mnemonic::random(&mut OsRng, Default::default());
        let (address, keypair) = key_derive::derive_key_pair_from_path(
            mnemonic.entropy(),
            None,
            &SignatureScheme::Secp256k1,
        )?;
        Ok((
            AccountId::new(address),
            keypair,
            mnemonic.phrase().to_string(),
        ))
    }

    fn get_home_dir(home: Option<PathBuf>) -> PathBuf {
        let home_dir = match home {
            Some(dir) => dir,
            None => dirs::home_dir().unwrap(),
        };
        home_dir
    }

    pub fn has_key(home: Option<PathBuf>) -> bool {
        let mut home_dir = Self::get_home_dir(home);
        home_dir.push(".db3");
        home_dir.push(".default");
        let key_path = home_dir.as_path();
        key_path.exists()
    }

    //
    // recover the from local filesystem
    //
    pub fn recover_keypair(home: Option<PathBuf>) -> std::io::Result<Self> {
        let mut home_dir = Self::get_home_dir(home);
        home_dir.push(".db3");
        let user_dir = home_dir.as_path();
        std::fs::create_dir_all(user_dir)?;
        home_dir.push(".default");
        let key_path = home_dir.as_path();
        if key_path.exists() {
            let kp_bytes = std::fs::read(key_path)?;
            let b64_str = std::str::from_utf8(kp_bytes.as_ref()).unwrap();
            let key_pair = DB3KeyPair::from_str(b64_str).unwrap();
            Ok(KeyStore::new(key_pair))
        } else {
            let (_, kp, _) = Self::generate_keypair().unwrap();
            let b64_str = kp.encode_base64();
            let mut f = File::create(key_path)?;
            f.write_all(b64_str.as_bytes())?;
            f.sync_all()?;
            Ok(KeyStore::new(kp))
        }
    }

    pub fn get_keypair(home: Option<PathBuf>) -> std::io::Result<DB3KeyPair> {
        if Self::has_key(home.clone()) {
            let mut home_dir = Self::get_home_dir(home.clone());
            home_dir.push(".db3");
            home_dir.push(".default");
            let key_path = home_dir.as_path();
            let kp_bytes = std::fs::read(key_path)?;
            let b64_str = std::str::from_utf8(kp_bytes.as_ref()).unwrap();
            let key_pair = DB3KeyPair::from_str(b64_str).unwrap();
            Ok(key_pair)
        } else {
            Err(Error::new(ErrorKind::Other, "no key was found"))
        }
    }

    pub fn get_address(&self) -> std::result::Result<DB3Address, String> {
        let pk = self.key_pair.public();

        Ok(DB3Address::from(&pk))
    }

    pub fn get_evm_address(&self) -> std::result::Result<DB3Address, String> {
        let b64_str = self.key_pair.encode_base64();
        let bytes: Vec<u8> = Base64::decode(b64_str.as_str()).unwrap();
        let offset_bytes = &(bytes[1..]);
        let private_key = hex::encode(offset_bytes);
        let wallet = private_key.parse::<LocalWallet>().unwrap();
        let address = wallet.address();
        let db3_address = DB3Address::try_from(address.0.as_ref()).unwrap();
        Ok(db3_address)
    }

    ///
    /// return hex string
    ///
    pub fn get_private_key(home: Option<PathBuf>) -> std::io::Result<String> {
        let kp = Self::get_keypair(home)?;
        let b64_str = kp.encode_base64();
        let bytes: Vec<u8> = Base64::decode(b64_str.as_str()).unwrap();
        let offset_bytes = &(bytes[1..]);
        Ok(hex::encode(offset_bytes))
    }

    pub fn show_key(&self) -> std::result::Result<Table, String> {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
        table.set_titles(row!["address", "scheme",]);
        let pk = self.key_pair.public();
        let id = AccountId::new(DB3Address::from(&pk));
        match &self.key_pair {
            DB3KeyPair::Ed25519(_) => {
                table.add_row(row![id.to_hex(), "ed25519"]);
            }
            DB3KeyPair::Secp256k1(_) => {
                table.add_row(row![id.to_hex(), "secp256k1"]);
            }
        }
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
