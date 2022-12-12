use db3_base::{get_a_static_keypair, get_address_from_pk};
use db3_error::{DB3Error, Result};
use ed25519_dalek::{Keypair, PublicKey, SecretKey, PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH};
use hex::FromHex;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::fs;
use std::option::Option;
use tendermint_config::NodeKey;

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
pub fn get_key_pair(file_path: Option<String>) -> Result<Keypair> {
    let mut home_dir = std::env::home_dir().unwrap();
    let key_path = match file_path {
        Some(path) => {
            home_dir.push(path);
            home_dir
        }
        None => {
            home_dir.push(".tendermint");
            home_dir.push("config");
            home_dir.push("node_key.json");
            home_dir
        }
    };

    match NodeKey::load_json_file(&key_path) {
        Ok(key_node) => match key_node.priv_key.ed25519_keypair() {
            Some(kp) => Ok(Keypair::from_bytes(kp.to_bytes().as_ref()).unwrap()),
            None => Err(DB3Error::LoadKeyPairError(format!(
                "parsed ed25519 keypair is null"
            ))),
        },
        Err(e) => Err(DB3Error::LoadKeyPairError(format!("{:?}", e))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn it_get_key_pair_with_default_path() {
        let res = get_key_pair(None);
        assert!(res.is_ok());
    }

    #[test]
    fn it_get_key_pair_file_not_exist() {
        let res = get_key_pair(Some("/node_key_not_exist_file.json".to_string()));
        assert!(res.is_err());
        println!("{:?}", res.err())
    }
}
