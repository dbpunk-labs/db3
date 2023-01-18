//
// db3_keypair.rs
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

use crate::db3_public_key::DB3PublicKey;
use crate::db3_signature::{
    DB3SignatureInner, Ed25519DB3Signature, Secp256k1DB3Signature, Signature,
};
use crate::signature_scheme::SignatureScheme;
use db3_error::DB3Error;
use derive_more::From;
use eyre::eyre;
use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PrivateKey};
use fastcrypto::encoding::{Base64, Encoding};
use fastcrypto::secp256k1::{Secp256k1KeyPair, Secp256k1PrivateKey};
pub use fastcrypto::traits::KeyPair as KeypairTraits;
pub use fastcrypto::traits::{
    AggregateAuthenticator, Authenticator, EncodeDecodeBase64, SigningKey, ToFromBytes,
    VerifyingKey,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::str::FromStr;

use signature::Signer;

#[derive(Debug, From)]
pub enum DB3KeyPair {
    Ed25519(Ed25519KeyPair),
    Secp256k1(Secp256k1KeyPair),
}

//
// multi signature schema keypair
//
impl DB3KeyPair {
    pub fn public(&self) -> DB3PublicKey {
        match self {
            DB3KeyPair::Ed25519(kp) => DB3PublicKey::Ed25519(kp.public().clone()),
            DB3KeyPair::Secp256k1(kp) => DB3PublicKey::Secp256k1(kp.public().clone()),
        }
    }
}

impl Signer<Signature> for DB3KeyPair {
    fn try_sign(&self, msg: &[u8]) -> std::result::Result<Signature, signature::Error> {
        match self {
            DB3KeyPair::Ed25519(kp) => kp.try_sign(msg),
            DB3KeyPair::Secp256k1(kp) => kp.try_sign(msg),
        }
    }
}

impl FromStr for DB3KeyPair {
    type Err = DB3Error;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let kp = Self::decode_base64(s).map_err(|e| DB3Error::KeyCodecError(format!("{}", e)))?;
        Ok(kp)
    }
}

impl EncodeDecodeBase64 for DB3KeyPair {
    ///
    /// encode keypair object to base64 string
    ///
    fn encode_base64(&self) -> String {
        let mut bytes: Vec<u8> = Vec::new();
        match self {
            DB3KeyPair::Ed25519(kp) => {
                let kp1 = kp.copy();
                bytes.extend_from_slice(&[self.public().flag()]);
                bytes.extend_from_slice(kp1.private().as_ref());
            }

            DB3KeyPair::Secp256k1(kp) => {
                let kp1 = kp.copy();
                bytes.extend_from_slice(&[self.public().flag()]);
                bytes.extend_from_slice(kp1.private().as_ref());
            }
        }
        Base64::encode(&bytes[..])
    }

    ///
    /// decode the base64 string keypair to object
    ///
    fn decode_base64(value: &str) -> std::result::Result<Self, eyre::Report> {
        let bytes = Base64::decode(value).map_err(|e| eyre!("{}", e.to_string()))?;
        match SignatureScheme::from_flag_byte(bytes.first().ok_or_else(|| eyre!("Invalid length"))?)
        {
            Ok(x) => match x {
                SignatureScheme::ED25519 => {
                    let sk = Ed25519PrivateKey::from_bytes(
                        bytes.get(1..).ok_or_else(|| eyre!("Invalid length"))?,
                    )
                    .map_err(|_| eyre!("invalid secret"))?;
                    let kp = Ed25519KeyPair::from(sk);
                    Ok(DB3KeyPair::Ed25519(kp))
                }
                SignatureScheme::Secp256k1 => {
                    let sk = Secp256k1PrivateKey::from_bytes(
                        bytes.get(1..).ok_or_else(|| eyre!("Invalid length"))?,
                    )
                    .map_err(|_| eyre!("invalid secret"))?;
                    let kp = Secp256k1KeyPair::from(sk);
                    Ok(DB3KeyPair::Secp256k1(kp))
                }
            },
            _ => Err(eyre!("Invalid bytes")),
        }
    }
}

impl Serialize for DB3KeyPair {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.encode_base64();
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for DB3KeyPair {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        <DB3KeyPair as EncodeDecodeBase64>::decode_base64(&s)
            .map_err(|e| Error::custom(e.to_string()))
    }
}

impl Signer<Signature> for Ed25519KeyPair {
    fn try_sign(&self, msg: &[u8]) -> std::result::Result<Signature, signature::Error> {
        Ok(Ed25519DB3Signature::new(self, msg)
            .map_err(|_| signature::Error::new())?
            .into())
    }
}

impl Signer<Signature> for Secp256k1KeyPair {
    fn try_sign(&self, msg: &[u8]) -> std::result::Result<Signature, signature::Error> {
        Ok(Secp256k1DB3Signature::new(self, msg)
            .map_err(|_| signature::Error::new())?
            .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
