import { ethers } from "hardhat";

async function main() {
  const DB3MetaStore = await ethers.getContractFactory("DB3MetaStore");
  const store = await DB3MetaStore.deploy();
  console.log(`store address ${store.address}`);
}

// We recommend this pattern to be able to use async/await everywhere
// and properly handle errors.
main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
