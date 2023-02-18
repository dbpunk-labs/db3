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

use db3_error::Result;

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
