//
// strings.rs
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

use db3_proto::db3_base_proto::{UnitType, Units};
const STORAGE_LABELS: [char; 7] = [' ', 'K', 'M', 'G', 'T', 'P', 'E'];
pub fn bytes_to_readable_num_str(bytes_size: u64) -> String {
    let max_shift = 7;
    let mut shift = 0;
    let mut local_bytes_size = bytes_size;
    let mut value: f64 = bytes_size as f64;
    local_bytes_size >>= 10;
    while local_bytes_size > 0 && shift < max_shift {
        value /= 1024.0;
        shift += 1;
        local_bytes_size >>= 10;
    }
    format!("{0:.2}{1}", value, STORAGE_LABELS[shift])
}

pub fn units_to_readable_num_str(units: &Units) -> String {
    if units.utype == UnitType::Tai as i32 {
        format!("{} tai", units.amount)
    } else {
        format!("{} db3", units.amount)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
