import { ethers } from "hardhat";

async function main() {
  const Token = await ethers.getContractFactory("Db3Token");
  const token = await Token.deploy();
  const Rollup = await ethers.getContractFactory("DB3Rollup");
  const rollup = await Rollup.deploy(token.address);
  console.log(`token address ${token.address} rollup address ${rollup.address}`);
}

// We recommend this pattern to be able to use async/await everywhere
// and properly handle errors.
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
