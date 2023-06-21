//
// db3_token_test.ts
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


import { expect } from "chai";
import hre from "hardhat";

describe("DB3 Token test", function () {
  it("get current total apply", async function () {
    const total_apply = 10_000_000_000_000;
    // deploy a lock contract where funds can be withdrawn
    // one year in the future
    const Token = await hre.ethers.getContractFactory("Db3Token");
    const token = await Token.deploy();
    // assert that the value is correct
    expect(await token.totalSupply()).to.equal(total_apply);
  });
});
