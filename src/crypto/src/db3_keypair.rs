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
    pub fn try_sign_hashed_message(&self, msg: &[u8]) -> std::result::Result<Vec<u8>, DB3Error> {
        match self {
            DB3KeyPair::Ed25519(_) => Err(DB3Error::SignError(
                "signing hashed message is not supperted with ed25519".to_string(),
            )),
            DB3KeyPair::Secp256k1(kp) => Secp256k1DB3Signature::new_hashed(&kp, msg),
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
    use crate::db3_signature::DB3Signature;
    use crate::key_derive;
    use bip39::{Language, Mnemonic, Seed};
    #[test]
    fn keypair_smoke_test_secp256k1() {
        let mnemonic = Mnemonic::from_phrase(
            "result crisp session latin must fruit genuine question prevent start coconut brave speak student dismiss",
            Language::English,
        )
        .unwrap();
        let seed = Seed::new(&mnemonic, "");
        let (address, keypair) =
            key_derive::derive_key_pair_from_path(seed.as_ref(), None, &SignatureScheme::Secp256k1)
                .unwrap();
        assert_eq!(
            "\"0xed17b3f435c03ff69c2cdc6d394932e68375f20f\"",
            serde_json::to_string(&address).unwrap()
        );
        let b64_str = keypair.encode_base64();
        assert_eq!("AcmPGAEAvTzN4yUK5TXNtDRq68nC2HY2cWy1IyOAViyi", b64_str);
        let keypair =
            DB3KeyPair::decode_base64("AaMABK0LhkIfC8Zk95K9hq8vIhSozAiEwRnNbpPT9DDt").unwrap();
        let msg: [u8; 1] = [0; 1];
        let result = keypair.try_sign(&msg);
        assert_eq!(true, result.is_ok());
        let signature = result.unwrap();
        assert_eq!(
            "\"AZ0DU4T3WgMT2AWsczEus1Xl08Q1VsTZu6pPE0e1Op1LRqEz+FL+FgKjQDlVU3r6HAKBImJ3OyHZd4cHFudcbzABAhZubOzkI26ArLlTxnxFZSQy67JFBaPlrFJe6aTA58Lw\"",
            serde_json::to_string(&signature).unwrap()
        );
        let result = signature.verify(&msg);
        assert_eq!(true, result.is_ok());

        let ts_signature = "AH5QFEhl8OQHom8DmzkWJeuPs62q3z7XhAcIUM+MwYnEMoOCA8tB4K4JcEZIqu4vHYu6H4/XHc6Wmn0L0m6TaCsBA+NxdDVYKrM9LjFdIem8ThlQCh/EyM3HOhU2WJF3SxMf";
        let ts_signature_obj_ret =
            Secp256k1DB3Signature::from_bytes(Base64::decode(ts_signature).unwrap().as_ref());
        assert_eq!(true, ts_signature_obj_ret.is_ok());
        let ts_address = ts_signature_obj_ret.unwrap().verify(&msg).unwrap();
        assert_eq!(
            "\"0xed17b3f435c03ff69c2cdc6d394932e68375f20f\"",
            serde_json::to_string(&ts_address).unwrap()
        );
    }

    #[test]
    fn keypair_smoke_test_ed25519() {
        let mnemonic = Mnemonic::from_phrase(
            "result crisp session latin must fruit genuine question prevent start coconut brave speak student dismiss",
            Language::English,
        )
        .unwrap();
        let seed = Seed::new(&mnemonic, "");
        let (address, keypair) =
            key_derive::derive_key_pair_from_path(seed.as_ref(), None, &SignatureScheme::ED25519)
                .unwrap();
        assert_eq!(
            "\"0x1a4623343cd42be47d67314fce0ad042f3c82685\"",
            serde_json::to_string(&address).unwrap()
        );
        let msg: [u8; 1] = [0; 1];
        let result = keypair.try_sign(&msg);
        assert_eq!(true, result.is_ok());
        let signature = result.unwrap();
        let result = signature.verify(&msg);
        assert_eq!(true, result.is_ok());
        let ts_signature = "AH5QFEhl8OQHom8DmzkWJeuPs62q3z7XhAcIUM+MwYnEMoOCA8tB4K4JcEZIqu4vHYu6H4/XHc6Wmn0L0m6TaCsBA+NxdDVYKrM9LjFdIem8ThlQCh/EyM3HOhU2WJF3SxMf";
        let ts_signature_obj_ret =
            Ed25519DB3Signature::from_bytes(Base64::decode(ts_signature).unwrap().as_ref());
        assert_eq!(false, ts_signature_obj_ret.is_ok());
        let ts_signature = "AGAxggujR0I6p1CFqT4iUlfRs++AgprT4gREHM71+V8qkRktNJRx4WOjudvKGiQUioJ6AU3WC/n1aJjKpa/NXA5oWy1vmHhN12MkmvIckvWIyhvoDECpjFW/fJG3TlrB4g==";
        let ts_signature_obj_ret =
            Ed25519DB3Signature::from_bytes(Base64::decode(ts_signature).unwrap().as_ref());
        assert_eq!(true, ts_signature_obj_ret.is_ok());

        let ts_address = ts_signature_obj_ret.unwrap().verify(&msg).unwrap();
        assert_eq!(
            "\"0x1a4623343cd42be47d67314fce0ad042f3c82685\"",
            serde_json::to_string(&ts_address).unwrap()
        );
    }

    #[test]
    fn keypair_ts_secp259k1_case() {
        let ts_signature = "AEAgHci5wbl0OEsqPVjjGAStTVZn3CbatXuAmF2KJ7jVDgYGk/t6Bdre99eNCEyfV3387dVY//D0+J8YuuXgI94BA+NxdDVYKrM9LjFdIem8ThlQCh/EyM3HOhU2WJF3SxMf";
        let msg = Base64::decode("CgUIt0oYCg==").unwrap();
        let ts_signature_ret =
            Secp256k1DB3Signature::from_bytes(Base64::decode(ts_signature).unwrap().as_ref());
        assert_eq!(true, ts_signature_ret.is_ok());
        let is_ok = ts_signature_ret.unwrap().verify(msg.as_ref());
        assert_eq!(true, is_ok.is_ok());
    }
}
