//
//
// db3_verifier.rs
// Copyright (C) 2022 db3.network Author imotai <codego.me@gmail.com>
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
use crate::db3_public_key::DB3PublicKey;
use crate::db3_signature::{DB3Signature, Signature};
use crate::id::AccountId;
use crate::signature_scheme::SignatureScheme;
use db3_error::{DB3Error, Result};
use fastcrypto::secp256k1::Secp256k1Signature;
use signature::Signature as _;

pub struct DB3Verifier {}

impl DB3Verifier {
    pub fn verify(msg: &[u8], signature_raw: &[u8]) -> Result<AccountId> {
        let signature = Signature::from_bytes(signature_raw)
            .map_err(|e| DB3Error::InvalidSignature(format!("{e}")))?;
        let db3_address = signature.verify(&msg)?;
        Ok(AccountId::new(db3_address))
    }

    pub fn verify_evm_hashed(hashed: &[u8], signature_raw: &[u8]) -> Result<AccountId> {
        let signature = Signature::from_bytes(signature_raw)
            .map_err(|e| DB3Error::InvalidSignature(format!("fail to generate signature {e}")))?;
        let spk =
            DB3PublicKey::try_from_bytes(SignatureScheme::Secp256k1, signature.public_key_bytes())
                .map_err(|e| DB3Error::InvalidSignature(format!("bad public key  {e}")))?;
        let sig = Secp256k1Signature::from_bytes(signature.signature_bytes())
            .map_err(|e| DB3Error::InvalidSignature(format!("bad signature scheme {e}")))?;
        if let Signature::Secp256k1DB3Signature(_) = signature {
            let db3_address = DB3Address::from_evm_public_key(&spk);
            if let DB3PublicKey::Secp256k1(internal_pk) = spk {
                internal_pk.verify_hashed(hashed, &sig).map_err(|e| {
                    DB3Error::InvalidSignature(format!("invalid hashed message for {e}"))
                })?;
                Ok(AccountId::new(db3_address))
            } else {
                Err(DB3Error::InvalidSignature("bad signature".to_string()))
            }
        } else {
            Err(DB3Error::InvalidSignature(
                "bad signature secp256k1 expected".to_string(),
            ))
        }
    }

    pub fn verify_hashed(hashed: &[u8], signature_raw: &[u8]) -> Result<AccountId> {
        let signature = Signature::from_bytes(signature_raw)
            .map_err(|e| DB3Error::InvalidSignature(format!("fail to generate signature {e}")))?;
        let spk =
            DB3PublicKey::try_from_bytes(SignatureScheme::Secp256k1, signature.public_key_bytes())
                .map_err(|e| DB3Error::InvalidSignature(format!("bad public key  {e}")))?;
        let sig = Secp256k1Signature::from_bytes(signature.signature_bytes())
            .map_err(|e| DB3Error::InvalidSignature(format!("bad signature scheme {e}")))?;
        if let Signature::Secp256k1DB3Signature(_) = signature {
            let db3_address = DB3Address::from(&spk);
            if let DB3PublicKey::Secp256k1(internal_pk) = spk {
                internal_pk.verify_hashed(hashed, &sig).map_err(|e| {
                    DB3Error::InvalidSignature(format!("invalid hashed message for {e}"))
                })?;
                Ok(AccountId::new(db3_address))
            } else {
                Err(DB3Error::InvalidSignature("bad signature".to_string()))
            }
        } else {
            Err(DB3Error::InvalidSignature(
                "bad signature secp256k1 expected".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastcrypto::encoding::{Base64, Encoding};
    #[test]
    fn db3_verifier_smoke_test() {
        let signature_b64 = "AUAgHci5wbl0OEsqPVjjGAStTVZn3CbatXuAmF2KJ7jVDgYGk/t6Bdre99eNCEyfV3387dVY//D0+J8YuuXgI94BA+NxdDVYKrM9LjFdIem8ThlQCh/EyM3HOhU2WJF3SxMf";
        let msg_b64 = "CgUIt0oYCg==";
        let signature = Base64::decode(signature_b64).unwrap();
        let msg = Base64::decode(msg_b64).unwrap();
        let result = DB3Verifier::verify(msg.as_ref(), signature.as_ref());
        assert_eq!(true, result.is_ok());
    }
}
