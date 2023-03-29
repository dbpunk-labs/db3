//
// db_owner_key.rs
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

use db3_crypto::db3_address::DB3Address;
use db3_error::Result;

const OWNER_PREFIX: &str = "/db_owner/";

pub struct DbOwnerKey<'a>(pub &'a DB3Address, pub u64, pub u16);

impl<'a> DbOwnerKey<'a> {
    ///
    /// encode the database key
    ///
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut encoded_key = OWNER_PREFIX.as_bytes().to_vec();
        encoded_key.extend_from_slice(self.0.as_ref());
        encoded_key.extend_from_slice(self.1.to_be_bytes().as_ref());
        encoded_key.extend_from_slice(self.2.to_be_bytes().as_ref());
        Ok(encoded_key)
    }

    #[allow(dead_code)]
    #[inline]
    pub fn max(owner: &DB3Address) -> Result<Vec<u8>> {
        let db_owner = DbOwnerKey(owner, std::u64::MAX, std::u16::MAX);
        db_owner.encode()
    }

    #[allow(dead_code)]
    #[inline]
    pub fn min(owner: &DB3Address) -> Result<Vec<u8>> {
        let db_owner = DbOwnerKey(owner, 0, 0);
        db_owner.encode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use db3_crypto::key_derive;
    use db3_crypto::signature_scheme::SignatureScheme;
    fn gen_address() -> DB3Address {
        let seed: [u8; 32] = [0; 32];
        let (address, _) =
            key_derive::derive_key_pair_from_path(&seed, None, &SignatureScheme::ED25519).unwrap();
        address
    }

    #[test]
    fn db_owner_key_happy_path() -> Result<()> {
        let addr = gen_address();
        let key = DbOwnerKey(&addr, 42, 1234);
        let encoded = key.encode()?;
        let expected_prefix = OWNER_PREFIX.as_bytes();
        let expected_addr = addr.as_ref();
        let expected_num1 = 42_u64.to_be_bytes();
        let expected_num2 = 1234_u16.to_be_bytes();
        assert_eq!(&encoded[..expected_prefix.len()], expected_prefix);
        assert_eq!(
            &encoded[expected_prefix.len()..expected_prefix.len() + expected_addr.len()],
            expected_addr
        );
        assert_eq!(
            &encoded[expected_prefix.len() + expected_addr.len()
                ..expected_prefix.len() + expected_addr.len() + 8],
            &expected_num1[..]
        );
        assert_eq!(
            &encoded[expected_prefix.len() + expected_addr.len() + 8
                ..expected_prefix.len() + expected_addr.len() + 10],
            &expected_num2[..]
        );
        Ok(())
    }
    #[test]
    fn test_encode_min() -> Result<()> {
        let addr = gen_address();
        let key = DbOwnerKey(&addr, 0, 0);

        let encoded = key.encode()?;
        let expected_prefix = OWNER_PREFIX.as_bytes();
        let expected_addr = addr.as_ref();
        let expected_num1 = 0_u64.to_be_bytes();
        let expected_num2 = 0_u16.to_be_bytes();

        assert_eq!(&encoded[..expected_prefix.len()], expected_prefix);
        assert_eq!(
            &encoded[expected_prefix.len()..expected_prefix.len() + expected_addr.len()],
            expected_addr
        );
        assert_eq!(
            &encoded[expected_prefix.len() + expected_addr.len()
                ..expected_prefix.len() + expected_addr.len() + 8],
            &expected_num1[..]
        );
        assert_eq!(
            &encoded[expected_prefix.len() + expected_addr.len() + 8
                ..expected_prefix.len() + expected_addr.len() + 10],
            &expected_num2[..]
        );

        Ok(())
    }

    #[test]
    fn test_encode_max() -> Result<()> {
        let addr = gen_address();
        let key = DbOwnerKey(&addr, std::u64::MAX, std::u16::MAX);

        let encoded = key.encode()?;
        let expected_prefix = OWNER_PREFIX.as_bytes();
        let expected_addr = addr.as_ref();
        let expected_num1 = std::u64::MAX.to_be_bytes();
        let expected_num2 = std::u16::MAX.to_be_bytes();

        assert_eq!(&encoded[..expected_prefix.len()], expected_prefix);
        assert_eq!(
            &encoded[expected_prefix.len()..expected_prefix.len() + expected_addr.len()],
            expected_addr
        );
        assert_eq!(
            &encoded[expected_prefix.len() + expected_addr.len()
                ..expected_prefix.len() + expected_addr.len() + 8],
            &expected_num1[..]
        );
        assert_eq!(
            &encoded[expected_prefix.len() + expected_addr.len() + 8
                ..expected_prefix.len() + expected_addr.len() + 10],
            &expected_num2[..]
        );

        Ok(())
    }
}
