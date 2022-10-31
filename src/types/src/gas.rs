//
// gas.rs
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
const SHIFT: [i64; 2] = [1, 1000_000];
const REVERSE_SHIFT: [i64; 2] = [1000_000, 1];

pub fn gas_cmp(left: &Units, right: &Units) -> std::cmp::Ordering {
    let mut left_value: i64 = left.amount;
    let mut right_value: i64 = right.amount;
    for _ in SHIFT {
        let index = (left.utype as i32 - UnitType::Db3 as i32) as usize;
        left_value = left_value % SHIFT[index];
        right_value = right_value % SHIFT[index];
        if left_value != right_value {
            return left_value.cmp(&right_value);
        }
    }
    std::cmp::Ordering::Equal
}

pub fn gas_add(left: &Units, right: &Units) -> Units {
    let mut amount: i64 = 0;
    let mut utype: UnitType = UnitType::Tai;
    let left_index = (left.utype as i32 - UnitType::Db3 as i32) as usize;
    amount = left.amount * REVERSE_SHIFT[left_index];
    let right_index = (right.utype as i32 - UnitType::Db3 as i32) as usize;
    amount = amount + right.amount * REVERSE_SHIFT[right_index];
    Units {
        utype: utype.into(),
        amount,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn it_gas_cmp_eq() {
        let left = Units {
            utype: UnitType::Db3.into(),
            amount: 1,
        };
        let right = Units {
            utype: UnitType::Db3.into(),
            amount: 1,
        };
        assert_eq!(gas_cmp(&left, &right), std::cmp::Ordering::Equal);
    }
}
