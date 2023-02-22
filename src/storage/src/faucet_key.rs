//
// faucet_key.rs
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

pub fn build_faucet_key(addr: &[u8], ts: u32) -> Result<Vec<u8>> {
    if addr.len() != 20 {
        return Err(DB3Error::KeyCodecError("bad address length".to_string()));
    }
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(addr);
    buf.extend_from_slice(&ts.to_be_bytes());
    Ok(buf)
}

pub fn decode_faucet_key(data: &[u8]) -> Result<(Vec<u8>, u32)> {
    if data.len() != 24 {
        return Err(DB3Error::KeyCodecError("bad data length".to_string()));
    }
    let addr = data[0..20].to_vec();
    let ts = (&data[20..])
        .read_u32::<BigEndian>()
        .map_err(|e| DB3Error::KeyCodecError(format!("{e}")))?;
    Ok((addr, ts))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn faucet_key_smoke_test() {
        let addr: [u8; 20] = [1; 20];
        let ts: u32 = 10000;
        let result = build_faucet_key(&addr, ts);
        assert!(result.is_ok());
        let data = result.unwrap();
        let (decoded_addr, decoded_ts) = decode_faucet_key(&data).unwrap();
        assert_eq!(&addr as &[u8], &decoded_addr as &[u8]);
        assert_eq!(ts, decoded_ts);
    }
}
