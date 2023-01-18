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

use crate::db3_address::DB3Address;
use crate::db3_public_key::DB3PublicKeyScheme;
use crate::db3_serde::Readable;
use crate::signature_scheme::SignatureScheme;
use db3_error::{DB3Error, Result};
use enum_dispatch::enum_dispatch;
use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PublicKey, Ed25519Signature};
use fastcrypto::encoding::{Base64, Encoding};
use fastcrypto::secp256k1::{Secp256k1KeyPair, Secp256k1PublicKey, Secp256k1Signature};
use fastcrypto::traits::KeyPair as KeypairTraits;
use fastcrypto::traits::{Authenticator, ToFromBytes, VerifyingKey};
use fastcrypto::Verifier;
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{serde_as, Bytes};
use signature::Signer;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;

#[enum_dispatch]
#[derive(Clone, JsonSchema, PartialEq, Eq, Hash)]
pub enum Signature {
    Ed25519DB3Signature,
    Secp256k1DB3Signature,
}

impl Serialize for Signature {
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

impl<'de> Deserialize<'de> for Signature {
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

impl AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        match self {
            Signature::Ed25519DB3Signature(sig) => sig.as_ref(),
            Signature::Secp256k1DB3Signature(sig) => sig.as_ref(),
        }
    }
}

impl signature::Signature for Signature {
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

impl Debug for Signature {
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

impl Default for Ed25519DB3Signature {
    fn default() -> Self {
        Self([0; Ed25519PublicKey::LENGTH + Ed25519Signature::LENGTH + 1])
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

impl DB3SignatureInner for Ed25519DB3Signature {
    type Sig = Ed25519Signature;
    type PubKey = Ed25519PublicKey;
    type KeyPair = Ed25519KeyPair;
    const LENGTH: usize = Ed25519PublicKey::LENGTH + Ed25519Signature::LENGTH + 1;
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
        if bytes.len() != Self::LENGTH {
            return Err(signature::Error::new());
        }
        let mut sig_bytes = [0; Self::LENGTH];
        sig_bytes.copy_from_slice(bytes);
        Ok(Self(sig_bytes))
    }
}

impl DB3SignatureInner for Secp256k1DB3Signature {
    type Sig = Secp256k1Signature;
    type PubKey = Secp256k1PublicKey;
    type KeyPair = Secp256k1KeyPair;
    const LENGTH: usize = Secp256k1PublicKey::LENGTH + Secp256k1Signature::LENGTH + 1;
}

pub trait DB3SignatureInner: Sized + signature::Signature + PartialEq + Eq + Hash {
    type Sig: Authenticator<PubKey = Self::PubKey>;
    type PubKey: VerifyingKey<Sig = Self::Sig> + DB3PublicKeyScheme;
    type KeyPair: KeypairTraits<PubKey = Self::PubKey, Sig = Self::Sig>;
    const LENGTH: usize = Self::Sig::LENGTH + Self::PubKey::LENGTH + 1;
    const SCHEME: SignatureScheme = Self::PubKey::SIGNATURE_SCHEME;
    fn get_verification_inputs(&self, author: DB3Address) -> Result<(Self::Sig, Self::PubKey)> {
        // Is this signature emitted by the expected author?
        let bytes = self.public_key_bytes();
        let pk = Self::PubKey::from_bytes(bytes)
            .map_err(|_| DB3Error::KeyCodecError("Invalid public key".to_string()))?;
        let received_addr = DB3Address::from(&pk);
        if received_addr != author {
            return Err(DB3Error::InvalidSigner);
        }
        // deserialize the signature
        let signature = Self::Sig::from_bytes(self.signature_bytes())
            .map_err(|err| DB3Error::InvalidSignature(err.to_string()))?;
        Ok((signature, pk))
    }

    fn new(kp: &Self::KeyPair, message: &[u8]) -> Result<Self> {
        let sig = kp.try_sign(message).map_err(|_| {
            DB3Error::InvalidSignature("Failed to sign valid message with keypair".to_string())
        })?;

        let mut signature_bytes: Vec<u8> = Vec::new();
        signature_bytes
            .extend_from_slice(&[<Self::PubKey as DB3PublicKeyScheme>::SIGNATURE_SCHEME.flag()]);
        signature_bytes.extend_from_slice(sig.as_ref());
        signature_bytes.extend_from_slice(kp.public().as_ref());
        Self::from_bytes(&signature_bytes[..])
            .map_err(|err| DB3Error::InvalidSignature(err.to_string()))
    }
}

#[enum_dispatch(Signature)]
pub trait DB3Signature: Sized + signature::Signature {
    fn signature_bytes(&self) -> &[u8];
    fn public_key_bytes(&self) -> &[u8];
    fn scheme(&self) -> SignatureScheme;

    fn verify(&self, value: &[u8], author: DB3Address) -> Result<()>;
}

pub trait Signable<W> {
    fn write(&self, writer: &mut W);
}

impl<S: DB3SignatureInner + Sized> DB3Signature for S {
    fn signature_bytes(&self) -> &[u8] {
        // Access array slice is safe because the array bytes is initialized as
        // flag || signature || pubkey with its defined length.
        &self.as_ref()[1..1 + S::Sig::LENGTH]
    }

    fn public_key_bytes(&self) -> &[u8] {
        // Access array slice is safe because the array bytes is initialized as
        // flag || signature || pubkey with its defined length.
        &self.as_ref()[S::Sig::LENGTH + 1..]
    }

    fn scheme(&self) -> SignatureScheme {
        S::PubKey::SIGNATURE_SCHEME
    }

    fn verify(&self, value: &[u8], author: DB3Address) -> Result<()> {
        // Currently done twice - can we improve on this?;
        let (sig, pk) = &self.get_verification_inputs(author)?;
        pk.verify(value, sig)
            .map_err(|e| DB3Error::InvalidSignature(format!("{}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key_derive;

    #[test]
    fn secp256k1_signature_smoke_test() {
        let seed: [u8; 32] = [0; 32];
        let (address, keypair) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::Secp256k1)
                .unwrap();
        let msg: [u8; 1] = [0; 1];
        let result = keypair.try_sign(&msg);
        assert_eq!(true, result.is_ok());
        let signature = result.unwrap();
        // as ref
        let result = signature.verify(&msg, address);
        assert_eq!(true, result.is_ok());
        let byte_data = signature.as_ref();
        let result = Signature::from_bytes(byte_data);
        assert_eq!(true, result.is_ok());
        let signature = result.unwrap();
        let result = signature.verify(&msg, address);
        assert_eq!(true, result.is_ok());
    }

    #[test]
    fn ed25119_signature_smoke_test() {
        let seed: [u8; 32] = [0; 32];
        let (address, keypair) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::ED25519).unwrap();
        let msg: [u8; 1] = [0; 1];
        let result = keypair.try_sign(&msg);
        assert_eq!(true, result.is_ok());
        let signature = result.unwrap();
        // as ref
        let result = signature.verify(&msg, address);
        assert_eq!(true, result.is_ok());
        let byte_data = signature.as_ref();
        let result = Signature::from_bytes(byte_data);
        assert_eq!(true, result.is_ok());
        let signature = result.unwrap();
        let result = signature.verify(&msg, address);
        assert_eq!(true, result.is_ok());
    }
}
