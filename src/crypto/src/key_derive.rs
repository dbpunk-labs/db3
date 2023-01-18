//
// key_derive.rs
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

use crate::{db3_address::DB3Address, db3_keypair::DB3KeyPair, signature_scheme::SignatureScheme};
use bip32::{ChildNumber, DerivationPath, XPrv};
use db3_error::{DB3Error, Result};
use fastcrypto::ed25519::Ed25519KeyPair;
use fastcrypto::{
    ed25519::Ed25519PrivateKey,
    secp256k1::{Secp256k1KeyPair, Secp256k1PrivateKey},
    traits::{KeyPair, ToFromBytes},
};
use slip10_ed25519::derive_ed25519_private_key;

pub const DERIVATION_PATH_COIN_TYPE: u32 = 784;
pub const DERVIATION_PATH_PURPOSE_ED25519: u32 = 44;
pub const DERVIATION_PATH_PURPOSE_SECP256K1: u32 = 54;

/// Ed25519 follows SLIP-0010 using hardened path: m/44'/784'/0'/0'/{index}'
/// Secp256k1 follows BIP-32 using path where the first 3 levels are hardened: m/54'/784'/0'/0/{index}
/// Note that the purpose for Secp256k1 is registered as 54, to differentiate from Ed25519 with purpose 44.

pub fn derive_key_pair_from_path(
    seed: &[u8],
    derivation_path: Option<DerivationPath>,
    key_scheme: &SignatureScheme,
) -> Result<(DB3Address, DB3KeyPair)> {
    let path = validate_path(key_scheme, derivation_path)?;
    match key_scheme {
        SignatureScheme::ED25519 => {
            let indexes = path.into_iter().map(|i| i.into()).collect::<Vec<_>>();
            let derived = derive_ed25519_private_key(seed, &indexes);
            let sk = Ed25519PrivateKey::from_bytes(&derived)
                .map_err(|e| DB3Error::SignatureKeyGenError(e.to_string()))?;
            let kp = Ed25519KeyPair::from(sk);
            Ok((kp.public().into(), DB3KeyPair::Ed25519(kp)))
        }
        SignatureScheme::Secp256k1 => {
            let child_xprv = XPrv::derive_from_path(seed, &path)
                .map_err(|e| DB3Error::SignatureKeyGenError(e.to_string()))?;
            let kp = Secp256k1KeyPair::from(
                Secp256k1PrivateKey::from_bytes(child_xprv.private_key().to_bytes().as_slice())
                    .unwrap(),
            );
            Ok((kp.public().into(), DB3KeyPair::Secp256k1(kp)))
        }
    }
}

pub fn validate_path(
    key_scheme: &SignatureScheme,
    path: Option<DerivationPath>,
) -> Result<DerivationPath> {
    match key_scheme {
        SignatureScheme::ED25519 => {
            match path {
                Some(p) => {
                    // The derivation path must be hardened at all levels with purpose = 44, coin_type = 784
                    if let &[purpose, coin_type, account, change, address] = p.as_ref() {
                        if purpose
                            == ChildNumber::new(DERVIATION_PATH_PURPOSE_ED25519, true).unwrap()
                            && coin_type
                                == ChildNumber::new(DERIVATION_PATH_COIN_TYPE, true).unwrap()
                            && account.is_hardened()
                            && change.is_hardened()
                            && address.is_hardened()
                        {
                            Ok(p)
                        } else {
                            Err(DB3Error::SignatureKeyGenError("Invalid path".to_string()))
                        }
                    } else {
                        Err(DB3Error::SignatureKeyGenError("Invalid path".to_string()))
                    }
                }
                None => Ok(format!(
                    "m/{DERVIATION_PATH_PURPOSE_ED25519}'/{DERIVATION_PATH_COIN_TYPE}'/0'/0'/0'"
                )
                .parse()
                .unwrap()),
            }
        }
        SignatureScheme::Secp256k1 => {
            match path {
                Some(p) => {
                    // The derivation path must be hardened at first 3 levels with purpose = 54, coin_type = 784
                    if let &[purpose, coin_type, account, change, address] = p.as_ref() {
                        if purpose
                            == ChildNumber::new(DERVIATION_PATH_PURPOSE_SECP256K1, true).unwrap()
                            && coin_type
                                == ChildNumber::new(DERIVATION_PATH_COIN_TYPE, true).unwrap()
                            && account.is_hardened()
                            && !change.is_hardened()
                            && !address.is_hardened()
                        {
                            Ok(p)
                        } else {
                            Err(DB3Error::SignatureKeyGenError("Invalid path".to_string()))
                        }
                    } else {
                        Err(DB3Error::SignatureKeyGenError("Invalid path".to_string()))
                    }
                }
                None => Ok(format!(
                    "m/{DERVIATION_PATH_PURPOSE_SECP256K1}'/{DERIVATION_PATH_COIN_TYPE}'/0'/0/0"
                )
                .parse()
                .unwrap()),
            }
        }
    }
}
