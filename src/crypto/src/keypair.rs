//
// keypair.rs
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

use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PrivateKey, Ed25519PublicKey, Ed25519Signature};
use fastcrypto::secp256k1::{
    Secp256k1KeyPair, Secp256k1PrivateKey, Secp256k1PublicKey, Secp256k1Signature,
};
pub use fastcrypto::traits::KeyPair as KeypairTraits;
pub use fastcrypto::traits::{
    AggregateAuthenticator, Authenticator, EncodeDecodeBase64, SigningKey, ToFromBytes,
    VerifyingKey,
};
use fastcrypto::Verifier;

#[derive(Debug, From)]
pub enum DB3KeyPair {
    Ed25519(Ed25519KeyPair),
    Secp256k1(Secp256k1KeyPair),
}

#[derive(Debug, Clone, PartialEq, Eq, From)]
pub enum PublicKey {
    Ed25519(Ed25519PublicKey),
    Secp256k1(Secp256k1PublicKey),
}

impl DB3KeyPair {
    pub fn public(&self) -> PublicKey {
        match self {
            DB3KeyPair::Ed25519(kp) => PublicKey::Ed25519(kp.public().clone()),
            DB3KeyPair::Secp256k1(kp) => PublicKey::Secp256k1(kp.public().clone()),
        }
    }
}

impl Signer<Signature> for DB3KeyPair {
    fn try_sign(&self, msg: &[u8]) -> Result<Signature, signature::Error> {
        match self {
            DB3KeyPair::Ed25519(kp) => kp.try_sign(msg),
            DB3KeyPair::Secp256k1(kp) => kp.try_sign(msg),
        }
    }
}

impl FromStr for DB3KeyPair {
    type Err = eyre::Report;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kp = Self::decode_base64(s).map_err(|e| eyre::eyre!("{}", e.to_string()))?;
        Ok(kp)
    }
}

impl EncodeDecodeBase64 for DB3KeyPair {
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

    fn decode_base64(value: &str) -> Result<Self, eyre::Report> {
        let bytes = Base64::decode(value).map_err(|e| eyre::eyre!("{}", e.to_string()))?;
        match bytes.first() {
            Some(x) => {
                if x == &Ed25519SuiSignature::SCHEME.flag() {
                    let priv_key_bytes = bytes
                        .get(1 + Ed25519PublicKey::LENGTH..)
                        .ok_or_else(|| eyre::eyre!("Invalid length"))?;
                    let sk = Ed25519PrivateKey::from_bytes(priv_key_bytes)?;
                    Ok(SuiKeyPair::Ed25519(<Ed25519KeyPair as From<
                        Ed25519PrivateKey,
                    >>::from(sk)))
                } else if x == &Secp256k1SuiSignature::SCHEME.flag() {
                    let sk = Secp256k1PrivateKey::from_bytes(
                        bytes
                            .get(1 + Secp256k1PublicKey::LENGTH..)
                            .ok_or_else(|| eyre::eyre!("Invalid length"))?,
                    )?;
                    Ok(SuiKeyPair::Secp256k1(<Secp256k1KeyPair as From<
                        Secp256k1PrivateKey,
                    >>::from(sk)))
                } else {
                    Err(eyre::eyre!("Invalid flag byte"))
                }
            }
            _ => Err(eyre::eyre!("Invalid bytes")),
        }
    }
}

impl Serialize for DB3KeyPair {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.encode_base64();
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for DB3KeyPair {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        <DB3KeyPair as EncodeDecodeBase64>::decode_base64(&s)
            .map_err(|e| Error::custom(e.to_string()))
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        match self {
            PublicKey::Ed25519(pk) => pk.as_ref(),
            PublicKey::Secp256k1(pk) => pk.as_ref(),
        }
    }
}

impl EncodeDecodeBase64 for PublicKey {
    fn encode_base64(&self) -> String {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.extend_from_slice(&[self.flag()]);
        bytes.extend_from_slice(self.as_ref());
        Base64::encode(&bytes[..])
    }

    fn decode_base64(value: &str) -> Result<Self, eyre::Report> {
        let bytes = Base64::decode(value).map_err(|e| eyre::eyre!("{}", e.to_string()))?;
        match bytes.first() {
            Some(x) => {
                if x == &<Ed25519PublicKey as SuiPublicKey>::SIGNATURE_SCHEME.flag() {
                    let pk = Ed25519PublicKey::from_bytes(
                        bytes
                            .get(1..)
                            .ok_or_else(|| eyre::eyre!("Invalid length"))?,
                    )?;
                    Ok(PublicKey::Ed25519(pk))
                } else if x == &<Secp256k1PublicKey as SuiPublicKey>::SIGNATURE_SCHEME.flag() {
                    let pk = Secp256k1PublicKey::from_bytes(
                        bytes
                            .get(1..)
                            .ok_or_else(|| eyre::eyre!("Invalid length"))?,
                    )?;
                    Ok(PublicKey::Secp256k1(pk))
                } else {
                    Err(eyre::eyre!("Invalid flag byte"))
                }
            }
            _ => Err(eyre::eyre!("Invalid bytes")),
        }
    }
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.encode_base64();
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        <PublicKey as EncodeDecodeBase64>::decode_base64(&s)
            .map_err(|e| Error::custom(e.to_string()))
    }
}

impl PublicKey {
    pub fn flag(&self) -> u8 {
        match self {
            PublicKey::Ed25519(_) => Ed25519SuiSignature::SCHEME.flag(),
            PublicKey::Secp256k1(_) => Secp256k1SuiSignature::SCHEME.flag(),
        }
    }

    pub fn try_from_bytes(
        curve: SignatureScheme,
        key_bytes: &[u8],
    ) -> Result<PublicKey, eyre::Report> {
        match curve {
            SignatureScheme::ED25519 => {
                Ok(PublicKey::Ed25519(Ed25519PublicKey::from_bytes(key_bytes)?))
            }
            SignatureScheme::Secp256k1 => Ok(PublicKey::Secp256k1(Secp256k1PublicKey::from_bytes(
                key_bytes,
            )?)),
            _ => Err(eyre::eyre!("Unsupported curve")),
        }
    }
    pub fn scheme(&self) -> SignatureScheme {
        match self {
            PublicKey::Ed25519(_) => Ed25519SuiSignature::SCHEME,
            PublicKey::Secp256k1(_) => Secp256k1SuiSignature::SCHEME,
        }
    }
}

impl DB3PublicKey for Ed25519PublicKey {
    const SIGNATURE_SCHEME: SignatureScheme = SignatureScheme::ED25519;
}
impl DB3PublicKey for Secp256k1PublicKey {
    const SIGNATURE_SCHEME: SignatureScheme = SignatureScheme::Secp256k1;
}

#[enum_dispatch]
#[derive(Clone, JsonSchema, PartialEq, Eq, Hash)]
pub enum DB3Signature {
    Ed25519DB3Signature,
    Secp256k1DB3Signature,
}

impl Serialize for DB3Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
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
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
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

impl DB3Signature {
    #[warn(deprecated)]
    pub fn new<T>(value: &T, secret: &dyn Signer<Signature>) -> Signature
    where
        T: Signable<Vec<u8>>,
    {
        let mut message = Vec::new();
        value.write(&mut message);
        secret.sign(&message)
    }

    pub fn new_secure<T>(
        value: &IntentMessage<T>,
        secret: &dyn signature::Signer<Signature>,
    ) -> Self
    where
        T: Serialize,
    {
        secret.sign(&bcs::to_bytes(&value).expect("Message serialization should not fail"))
    }
}

impl AsRef<[u8]> for DB3Signature {
    fn as_ref(&self) -> &[u8] {
        match self {
            Signature::Ed25519DB3Signature(sig) => sig.as_ref(),
            Signature::Secp256k1DB3Signature(sig) => sig.as_ref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
