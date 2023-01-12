//
// db3_serde.rs
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

use std::fmt::Debug;
use std::marker::PhantomData;

use anyhow::anyhow;
use fastcrypto::encoding::{Base64, Encoding};
use fastcrypto::traits::ToFromBytes;
use serde;
use serde::de::{Deserializer, Error};
use serde::ser::{Error as SerError, Serializer};
use serde::Deserialize;
use serde::Serialize;
use serde_with::{Bytes, DeserializeAs, SerializeAs};

/// Use with serde_as to encode/decode bytes to/from Base64/Hex for human-readable serializer and deserializer
/// E : Encoding of the human readable output
/// R : serde_as SerializeAs/DeserializeAs delegation
///
/// # Example:
///
/// ```text
/// #[serde_as]
/// #[derive(Deserialize, Serialize)]
/// struct Example(#[serde_as(as = "Readable(Hex, _)")] [u8; 20]);
/// ```
///
/// The above example will encode the byte array to Hex string for human-readable serializer
/// and array tuple (default) for non-human-readable serializer.
pub struct Readable<E, R> {
    element: PhantomData<R>,
    encoding: PhantomData<E>,
}

impl<T, R, E> SerializeAs<T> for Readable<E, R>
where
    T: AsRef<[u8]>,
    R: SerializeAs<T>,
    E: SerializeAs<T>,
{
    fn serialize_as<S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            E::serialize_as(value, serializer)
        } else {
            R::serialize_as(value, serializer)
        }
    }
}

/// DeserializeAs support for Arrays
impl<'de, R, E, const N: usize> DeserializeAs<'de, [u8; N]> for Readable<E, R>
where
    R: DeserializeAs<'de, [u8; N]>,
    E: DeserializeAs<'de, Vec<u8>>,
{
    fn deserialize_as<D>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let value = E::deserialize_as(deserializer)?;
            if value.len() != N {
                return Err(Error::custom(anyhow!(
                    "invalid array length {}, expecting {}",
                    value.len(),
                    N
                )));
            }
            let mut array = [0u8; N];
            array.copy_from_slice(&value[..N]);
            Ok(array)
        } else {
            R::deserialize_as(deserializer)
        }
    }
}

/// DeserializeAs support for Vec
impl<'de, R, E> DeserializeAs<'de, Vec<u8>> for Readable<E, R>
where
    R: DeserializeAs<'de, Vec<u8>>,
    E: DeserializeAs<'de, Vec<u8>>,
{
    fn deserialize_as<D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            E::deserialize_as(deserializer)
        } else {
            R::deserialize_as(deserializer)
        }
    }
}
