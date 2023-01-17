//
// db3_signature.rs
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


use crate::db3_serde::Readable;
use db3_error::{DB3Error, Result};
use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PublicKey, Ed25519Signature};
use fastcrypto::encoding::{Base64, Encoding};
use fastcrypto::secp256k1::{Secp256k1KeyPair, Secp256k1PublicKey, Secp256k1Signature};
use fastcrypto::traits::{Authenticator, KeyPair, VerifyingKey};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use signature::Signature;
use std::fmt::Debug;

// the byte size of secp256k1 signature
const SECP256K1_SIGNATURE_LENGTH: usize =
    Secp256k1PublicKey::LENGTH + Secp256k1Signature::LENGTH + 1;
// the byte size 0f ed25519 signature
const ED25519_SIGNATURE_LENGTH: usize = Ed25519PublicKey::LENGTH + Ed25519Signature::LENGTH + 1;


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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
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
    fn new(kp: &Ed25519KeyPair, message: &[u8]) -> Result<Self> {
        let sig = kp.try_sign(message).map_err(|_| {
            DB3Error::InvalidSignature("Failed to sign valid message with keypair".to_string())
        })?;
        let mut signature_bytes: Vec<u8> = Vec::new();
        signature_bytes.extend_from_slice(&[SignatureScheme::ED25519.flag()]);
        signature_bytes.extend_from_slice(sig.as_ref());
        signature_bytes.extend_from_slice(kp.public().as_ref());
        Self::from_bytes(&signature_bytes[..])
            .map_err(|err| DB3Error::InvalidSignature(err.to_string()))
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

//
// Secp256k1 DB3 Signature port
//
#[serde_as]
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Hash)]
pub struct Secp256k1DB3Signature(
    #[schemars(with = "Base64")]
    #[serde_as(as = "Readable<Base64, Bytes>")]
    [u8; Secp256k1PublicKey::LENGTH + Secp256k1Signature::LENGTH + 1],
);


impl Secp256k1DB3Signature {
    fn new(kp: &Secp256k1KeyPair, message: &[u8]) -> Result<Self> {
        let sig = kp.try_sign(message).map_err(|_| {
            DB3Error::InvalidSignature("Failed to sign valid message with keypair".to_string())
        })?;
        let mut signature_bytes: Vec<u8> = Vec::new();
        signature_bytes.extend_from_slice(&[SignatureScheme::Secp256k1.flag()]);
        signature_bytes.extend_from_slice(sig.as_ref());
        signature_bytes.extend_from_slice(kp.public().as_ref());
        Self::from_bytes(&signature_bytes[..])
            .map_err(|err| DB3Error::InvalidSignature(err.to_string()))
    }
}

impl AsRef<[u8]> for Secp256k1DB3Signature {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl AsMut<[u8]> for Secp256k1DB3Signature {
    fn as_mut(&mut self) -> &mut [u8] {
        self.0.as_mut()
    }
}

impl signature::Signature for Secp256k1DB3Signature {
    fn from_bytes(bytes: &[u8]) -> std::result::Result<Self, signature::Error> {
        if bytes.len() != SECP256K1_SIGNATURE_LENGTH {
            return Err(signature::Error::new());
        }
        let mut sig_bytes = [0; SECP256K1_SIGNATURE_LENGTH];
        sig_bytes.copy_from_slice(bytes);
        Ok(Self(sig_bytes))
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_works() {}
}
