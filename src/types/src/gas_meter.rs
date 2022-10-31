//
// gas_meter.rs
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
//

use db3_error::{DB3Error, Result};
use db3_proto::db3_base_proto::{Unit, UnitType};
use super::gas;

pub struct GasMeter {
    limited_gas: Unit,
    consumed_gas: Unit,
}

impl GasMeter {

    pub fn new(limited_gas:Unit)->Self {
        Self {
            limited_gas,
            consumed_gas: Unit{
                utype: UnitType.TAI.into(),
                amount:0
            }
        }
    }

    pub fn is_out_of_gas(&self) -> bool {
        let order = gas::gas_cmp(&self.consumed_gas, &self.limited_gas);
        if order == std::cmp::Ordering::Less {
            return false;
        }
        return true;
    }

    pub fn consume_gas(&mut self, amount:Units)-> Result<()> {
        let cloned_consumed_gas = self.consumed_gas.clone();
        let new_consumed_gas = gas::gas_add(new_consumed_gas, amount);
        let order = gas::gas_cmp(&new_consumed_gas, &self.limited_gas);
        if order == std::cmp::Ordering::Less {
            self.consumed_gas.amount = new_consumed_gas.amount;
            self.consumed_gas.utype = new_consumed_gas.utype;
            Ok(())
        }else {
            Err(DB3Error::OutOfGasError("exceed the limit of gas ".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn it_works() {
	}
}
