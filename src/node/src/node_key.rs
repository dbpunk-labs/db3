use db3_error::{DB3Error, Result};
use ed25519_dalek::Keypair;
use std::option::Option;
use tendermint_config::{PrivValidatorKey};

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
            home_dir.push("priv_validator_key.json");
            home_dir
        }
    };

    match PrivValidatorKey::load_json_file(&key_path) {
        Ok(key) => match key.priv_key.ed25519_keypair() {
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
