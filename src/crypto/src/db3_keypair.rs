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
use crate::signature_schema::SignatureScheme;
use db3_error::{DB3Error, Result};
use eyre::eyre;
use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PublicKey};
use fastcrypto::encoding::Base64;
use fastcrypto::secp256k1::{Secp256k1KeyPair, Secp256k1PublicKey};
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

impl Signer<DB3Signature> for DB3KeyPair {
    fn try_sign(&self, msg: &[u8]) -> std::result::Result<DB3Signature, signature::Error> {
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
                bytes.extend_from_slice(kp.public().as_ref());
                bytes.extend_from_slice(kp1.private().as_ref());
            }
            DB3KeyPair::Secp256k1(kp) => {
                let kp1 = kp.copy();
                bytes.extend_from_slice(&[self.public().flag()]);
                bytes.extend_from_slice(kp.public().as_ref());
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
                SignatureScheme::ED25519 => Ok(DB3KeyPair::Ed25519(Ed25519KeyPair::from_bytes(
                    bytes.get(1..).ok_or_else(|| eyre!("Invalid length"))?,
                )?)),
                SignatureScheme::Secp256k1 => {
                    Ok(DB3KeyPair::Secp256k1(Secp256k1KeyPair::from_bytes(
                        bytes.get(1..).ok_or_else(|| eyre!("Invalid length"))?,
                    )?))
                }
                _ => Err(eyre!("Invalid flag byte")),
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

#[enum_dispatch]
#[derive(Clone, JsonSchema, PartialEq, Eq, Hash)]
pub enum DB3Signature {
    Ed25519DB3Signature,
    Secp256k1DB3Signature,
}

impl Serialize for DB3Signature {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.as_ref();
        if serializer.is_human_readable() {
            let s = Base64::encode(bytes);
            serializer.serialize_str(&s)
        } else {
            serializer.serialize_bytes(bytes)
        }
    }
}

impl<'de> Deserialize<'de> for DB3Signature {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let bytes = if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            Base64::decode(&s).map_err(|e| Error::custom(e.to_string()))?
        } else {
            let data: Vec<u8> = Vec::deserialize(deserializer)?;
            data
        };
        Self::from_bytes(&bytes).map_err(|e| Error::custom(e.to_string()))
    }
}

impl AsRef<[u8]> for DB3Signature {
    fn as_ref(&self) -> &[u8] {
        match self {
            DB3Signature::Ed25519DB3Signature(sig) => sig.as_ref(),
            DB3Signature::Secp256k1DB3Signature(sig) => sig.as_ref(),
        }
    }
}

impl signature::Signature for DB3Signature {
    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, signature::Error> {
        match bytes.first() {
            Some(x) => {
                if x == &SignatureScheme::ED25519.flag() {
                    Ok(<Ed25519DB3Signature as ToFromBytes>::from_bytes(bytes)
                        .map_err(|_| signature::Error::new())?
                        .into())
                } else if x == &SignatureScheme::Secp256k1.flag() {
                    Ok(<Secp256k1DB3Signature as ToFromBytes>::from_bytes(bytes)
                        .map_err(|_| signature::Error::new())?
                        .into())
                } else {
                    Err(signature::Error::new())
                }
            }
            _ => Err(signature::Error::new()),
        }
    }
}

impl Debug for DB3Signature {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let flag = Base64::encode([self.scheme().flag()]);
        let s = Base64::encode(self.signature_bytes());
        let p = Base64::encode(self.public_key_bytes());
        write!(f, "{flag}@{s}@{p}")?;
        Ok(())
    }
}

#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
pub struct Ed25519DB3Signature(
    #[schemars(with = "Base64")]
    #[serde_as(as = "Readable<Base64, Bytes>")]
    [u8; Ed25519PublicKey::LENGTH + Ed25519Signature::LENGTH + 1],
);

impl Ed25519DB3Signature {
    fn new(kp: &Self::KeyPair, message: &[u8]) -> SuiResult<Self> {
        let sig = kp
            .try_sign(message)
            .map_err(|_| SuiError::InvalidSignature {
                error: "Failed to sign valid message with keypair".to_string(),
            })?;

        let mut signature_bytes: Vec<u8> = Vec::new();
        signature_bytes
            .extend_from_slice(&[<Self::PubKey as SuiPublicKey>::SIGNATURE_SCHEME.flag()]);
        signature_bytes.extend_from_slice(sig.as_ref());
        signature_bytes.extend_from_slice(kp.public().as_ref());
        Self::from_bytes(&signature_bytes[..]).map_err(|err| SuiError::InvalidSignature {
            error: err.to_string(),
        })
    }
}

impl AsRef<[u8]> for Ed25519DB3Signature {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for Ed25519DB3Signature {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl signature::Signature for Ed25519DB3Signature {
    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, signature::Error> {
        if bytes.len() != Self::LENGTH {
            return Err(signature::Error::new());
        }
        let mut sig_bytes = [0; Self::LENGTH];
        sig_bytes.copy_from_slice(bytes);
        Ok(Self(sig_bytes))
    }
}

impl Signer<DB3Signature> for Ed25519KeyPair {
    fn try_sign(&self, msg: &[u8]) -> Result<DB3Signature, signature::Error> {
        Ok(
            Ed25519DB3Signature::new(self, msg, SignatureScheme::ED25519.flag())
                .map_err(|_| signature::Error::new())?
                .into(),
        )
    }
}

pub trait DB3SignatureInner: Sized + signature::Signature + PartialEq + Eq + Hash {
    type Sig: Authenticator<PubKey = Self::PubKey>;
    const LENGTH: usize = Self::Sig::LENGTH + Self::PubKey::LENGTH + 1;
    fn new(kp: &Self::KeyPair, message: &[u8], flag: u8) -> Result<Self> {
        let sig = kp
            .try_sign(message)
            .map_err(|_| SuiError::InvalidSignature {
                error: "Failed to sign valid message with keypair".to_string(),
            })?;

        let mut signature_bytes: Vec<u8> = Vec::new();
        signature_bytes.extend_from_slice(flag);
        signature_bytes.extend_from_slice(sig.as_ref());
        signature_bytes.extend_from_slice(kp.public().as_ref());
        Self::from_bytes(&signature_bytes[..]).map_err(|err| DB3Error::InvalidSignature {
            error: err.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
