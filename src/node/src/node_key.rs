use db3_base::{get_a_static_keypair, get_address_from_pk};
use db3_error::{DB3Error, Result};
use ed25519_dalek::{Keypair, PublicKey, SecretKey, PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH};
use hex::FromHex;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::fs;
use std::option::Option;
use std::path::Path;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Root {
    pub address: String,
    #[serde(rename = "pub_key")]
    pub pub_key: PubKey,
    #[serde(rename = "priv_key")]
    pub priv_key: PrivKey,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PubKey {
    #[serde(rename = "type")]
    pub type_field: String,
    pub value: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrivKey {
    #[serde(rename = "type")]
    pub type_field: String,
    pub value: String,
}

pub fn get_mock_key_pair() -> Result<Keypair> {
    Ok(get_a_static_keypair())
}

pub fn get_key_pair() -> Result<Keypair> {
    let mut home_dir = std::env::home_dir().unwrap();
    let key_path = match file_path {
        Some(path) => {
            home_dir.push(path);
            home_dir.as_path()
        }
        None => {
            home_dir.push(".tendermint");
            home_dir.push("config");
            home_dir.push("priv_validator_key.json");
            home_dir.as_path()
        }
    };

    if key_path.exists() {
        let file_content = std::fs::read_to_string(key_path).expect("file should open read only");
        let root: Root =
            serde_json::from_str(file_content.as_str()).expect("JSON was not well-formatted");
        println!("{:?}", root);

        let public: PublicKey = match root.pub_key.type_field.as_str() {
            "tendermint/PubKeyEd25519" => {
                let public_key: &[u8] = root.pub_key.value.as_bytes();
                let pub_bytes: Vec<u8> = FromHex::from_hex(public_key).unwrap();
                PublicKey::from_bytes(&pub_bytes[..PUBLIC_KEY_LENGTH]).unwrap()
            }
            _ => {
                return Err(DB3Error::LoadKeyPairError(format!(
                    "invalid pubic key type {}",
                    root.pub_key.type_field
                )));
            }
        };

        let secret: SecretKey = match root.priv_key.type_field.as_str() {
            "tendermint/PubKeyEd25519" => {
                let secret_key: &[u8] = root.priv_key.value.as_bytes();
                let sec_bytes: Vec<u8> = FromHex::from_hex(secret_key).unwrap();
                SecretKey::from_bytes(&sec_bytes[..SECRET_KEY_LENGTH]).unwrap()
            }
            _ => {
                return Err(DB3Error::LoadKeyPairError(format!(
                    "invalid private key type {}",
                    root.priv_key.type_field
                )));
            }
        };
        println!("secret: {:?}", secret);
        println!("public: {:?}", public);
        Ok(Keypair { secret, public })
    } else {
        Err(DB3Error::LoadKeyPairError(format!(
            "key file {:?} not exist ",
            key_path
        )))
    }
}

#[cfg(test)]
mod tests {}
