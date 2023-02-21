//
// event_key.rs
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

use byteorder::{BigEndian, ReadBytesExt};
use db3_error::{DB3Error, Result};

#[repr(u8)]
pub enum EventType {
    DepositEvent = 0,
}

pub fn build_event_key(
    event_type: EventType,
    chain_id: u32,
    block_id: u64,
    tx_id: &[u8],
) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(&(event_type as u8).to_be_bytes());
    buf.extend_from_slice(&chain_id.to_be_bytes());
    buf.extend_from_slice(&block_id.to_be_bytes());
    buf.extend_from_slice(tx_id);
    Ok(buf)
}

pub fn build_event_key_range(
    event_type: EventType,
    chain_id: u32,
    block_id: u64,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut start: Vec<u8> = Vec::new();
    let event_type_bytes = (event_type as u8).to_be_bytes();
    let chain_id_bytes = chain_id.to_be_bytes();
    let block_id_bytes = block_id.to_be_bytes();
    start.extend_from_slice(&event_type_bytes);
    start.extend_from_slice(&chain_id_bytes);
    start.extend_from_slice(&block_id_bytes);
    start.extend_from_slice(vec![0].as_ref());
    let mut end: Vec<u8> = Vec::new();
    end.extend_from_slice(&event_type_bytes);
    end.extend_from_slice(&chain_id_bytes);
    end.extend_from_slice(&block_id_bytes);
    end.extend_from_slice(vec![u8::MAX].as_ref());
    Ok((start, end))
}

pub fn decode_event_key(data: &[u8]) -> Result<(EventType, u32, u64)> {
    if data.len() <= 13 {
        return Err(DB3Error::KeyCodecError("bad data length".to_string()));
    }

    let event_type = (&data[0..])
        .read_u8()
        .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
    let chain_id = (&data[1..])
        .read_u32::<BigEndian>()
        .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
    let block_id = (&data[5..])
        .read_u64::<BigEndian>()
        .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
    match event_type {
        0 => Ok((EventType::DepositEvent, chain_id, block_id)),
        _ => Err(DB3Error::KeyCodecError("invalid eventy type".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_event_key() {
        let tx: Vec<u8> = vec![255, 255];
        let key1 = build_event_key(EventType::DepositEvent, 1, 20, tx.as_ref());
        assert!(key1.is_ok());
        let key2 = build_event_key(EventType::DepositEvent, 1, 21, tx.as_ref());
        assert!(key2.is_ok());
        let key1 = key1.unwrap();
        assert!(key1 > key2.unwrap());
        assert_eq!(key1.len(), 15);
    }
}
