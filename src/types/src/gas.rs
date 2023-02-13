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

use db3_error::{DB3Error, Result};
use db3_proto::db3_base_proto::{UnitType, Units};

const SHIFT: [i64; 2] = [1, 1000_000_000];
const REVERSE_SHIFT: [i64; 2] = [1000_000_000, 1];

const UNIT_TYPES: [i32; 2] = [1, 0];

pub fn gas_cmp(left: &Units, right: &Units) -> std::cmp::Ordering {
    let mut left_value: i64 = left.amount;
    let mut right_value: i64 = right.amount;
    for unit in UNIT_TYPES {
        let left_index = (unit - left.utype as i32) as usize;
        let right_index = (unit - right.utype as i32) as usize;
        left_value = left_value / SHIFT[left_index];
        right_value = right_value / SHIFT[right_index];
        if left_value != right_value {
            return left_value.cmp(&right_value);
        }
    }
    std::cmp::Ordering::Equal
}

pub fn gas_add(left: &Units, right: &Units) -> Units {
    let utype: UnitType = UnitType::Tai;
    let left_index = (UnitType::Db3 as i32 - left.utype as i32) as usize;
    let amount = left.amount * REVERSE_SHIFT[left_index];
    let right_index = (UnitType::Db3 as i32 - right.utype as i32) as usize;
    let amount = amount + right.amount * REVERSE_SHIFT[right_index];
    Units {
        utype: utype.into(),
        amount,
    }
}

pub fn gas_consume(left: &Units, right: &Units) -> Result<Units> {
    match gas_cmp(left, right) {
        std::cmp::Ordering::Equal => Ok(Units {
            utype: UnitType::Tai as i32,
            amount: 0,
        }),
        std::cmp::Ordering::Less => Err(DB3Error::OutOfGasError("not enough gas".to_string())),
        std::cmp::Ordering::Greater => {
            let utype: UnitType = UnitType::Tai;
            let left_index = (UnitType::Db3 as i32 - left.utype as i32) as usize;
            let amount = left.amount * REVERSE_SHIFT[left_index];
            let right_index = (UnitType::Db3 as i32 - right.utype as i32) as usize;
            let amount = amount - right.amount * REVERSE_SHIFT[right_index];
            Ok(Units {
                utype: utype.into(),
                amount,
            })
        }
    }
}

pub fn gas_in_tai(input: &Units) -> u64 {
    if input.utype == UnitType::Db3 as i32 {
        return (input.amount * SHIFT[1]) as u64;
    }
    return input.amount as u64;
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

    #[test]
    fn it_gas_add() {
        let left = Units {
            utype: UnitType::Db3.into(),
            amount: 1,
        };
        let right = Units {
            utype: UnitType::Tai.into(),
            amount: 1,
        };
        let new_units = gas_add(&left, &right);
        assert_eq!(1000_000_001, new_units.amount);
    }
    #[test]
    fn it_gas_consume() {
        let left = Units {
            utype: UnitType::Db3.into(),
            amount: 1,
        };
        let right = Units {
            utype: UnitType::Tai.into(),
            amount: 1,
        };
        let new_units = gas_consume(&left, &right).unwrap();
        assert_eq!(999_999_999, new_units.amount);
    }
}
