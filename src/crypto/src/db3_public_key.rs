//
// db3_public_key.rs
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

use crate::signature_scheme::SignatureScheme;
use derive_more::From;
use eyre::eyre;
use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PublicKey};
use fastcrypto::encoding::Base64;
use fastcrypto::encoding::Encoding;
use fastcrypto::secp256k1::{Secp256k1KeyPair, Secp256k1PublicKey};
pub use fastcrypto::traits::{EncodeDecodeBase64, ToFromBytes};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, PartialEq, Eq, From)]
pub enum DB3PublicKey {
    Ed25519(Ed25519PublicKey),
    Secp256k1(Secp256k1PublicKey),
}

impl AsRef<[u8]> for DB3PublicKey {
    fn as_ref(&self) -> &[u8] {
        match self {
            DB3PublicKey::Ed25519(pk) => pk.as_ref(),
            DB3PublicKey::Secp256k1(pk) => pk.as_ref(),
        }
    }
}

impl EncodeDecodeBase64 for DB3PublicKey {
    ///
    /// encode db3 publickey to base64 string
    ///
    fn encode_base64(&self) -> String {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(&[self.flag()]);
        bytes.extend_from_slice(self.as_ref());
        Base64::encode(&bytes[..])
    }

    ///
    /// decode base64 string to db3 publickey
    ///
    fn decode_base64(value: &str) -> std::result::Result<Self, eyre::Report> {
        let bytes = Base64::decode(value).map_err(|e| eyre!("{}", e.to_string()))?;
        match bytes.first() {
            Some(x) => {
                if x == &SignatureScheme::ED25519.flag() {
                    let pk = Ed25519PublicKey::from_bytes(
                        bytes.get(1..).ok_or_else(|| eyre!("Invalid length"))?,
                    )?;
                    Ok(DB3PublicKey::Ed25519(pk))
                } else if x == &SignatureScheme::Secp256k1.flag() {
                    let pk = Secp256k1PublicKey::from_bytes(
                        bytes.get(1..).ok_or_else(|| eyre!("Invalid length"))?,
                    )?;
                    Ok(DB3PublicKey::Secp256k1(pk))
                } else {
                    Err(eyre!("Invalid flag byte"))
                }
            }
            _ => Err(eyre!("Invalid bytes")),
        }
    }
}

impl Serialize for DB3PublicKey {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.encode_base64();
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for DB3PublicKey {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        <DB3PublicKey as EncodeDecodeBase64>::decode_base64(&s)
            .map_err(|e| Error::custom(e.to_string()))
    }
}

impl DB3PublicKey {
    pub fn flag(&self) -> u8 {
        self.scheme().flag()
    }

    pub fn try_from_bytes(
        curve: SignatureScheme,
        key_bytes: &[u8],
    ) -> std::result::Result<DB3PublicKey, eyre::Report> {
        match curve {
            SignatureScheme::ED25519 => Ok(DB3PublicKey::Ed25519(Ed25519PublicKey::from_bytes(
                key_bytes,
            )?)),
            SignatureScheme::Secp256k1 => Ok(DB3PublicKey::Secp256k1(
                Secp256k1PublicKey::from_bytes(key_bytes)?,
            )),
            _ => Err(eyre::eyre!("Unsupported curve")),
        }
    }

    pub fn scheme(&self) -> SignatureScheme {
        match self {
            DB3PublicKey::Ed25519(_) => SignatureScheme::ED25519,
            DB3PublicKey::Secp256k1(_) => SignatureScheme::Secp256k1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {}
}
