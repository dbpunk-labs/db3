import { expect } from "chai";
import { ethers } from "hardhat";
import { DB3MetaStore, Events } from "../typechain-types";

describe("DB3MetaStore", function () {
  let metaStore: DB3MetaStore;
  let deployer: any;
  let sender: any;
  beforeEach(async function () {
    [deployer, sender] = await ethers.getSigners();
    const MetaStore = await ethers.getContractFactory("DB3MetaStore");
    metaStore = await MetaStore.deploy();
    await metaStore.deployed();
  });
  describe("update rollup steps", function () {
    it("update rollup steps invalid network", async function () {
      const binaryData = ethers.utils.base64.decode(
        "TY5SMaPPRk_TMvSDROaQWyc_WHyJrEL760-UhiNnHG4"
      );
      const arTx = ethers.utils.hexZeroPad(binaryData, 32);
      await expect(
        metaStore.connect(deployer).updateRollupSteps(1, arTx)
      ).to.revertedWith("Data Network is not registered");
    });

    it("update rollup steps ar tx", async function () {
      const eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
      const hello = ethers.utils.formatBytes32String("hello");
      await expect(
        metaStore
          .connect(deployer)
          .registerDataNetwork(
            "https://rollup.com",
            deployer.address,
            ["https://index-node-1.com", "https://index-node-2.com"],
            [sender.address, deployer.address],
            hello
          )
      )
        .to.emit(eventLibABI, "CreateNetwork")
        .withArgs(deployer.address, 1);
      const arTx = ethers.utils.formatBytes32String("");
      await expect(
        metaStore.connect(deployer).updateRollupSteps(1, arTx)
      ).to.revertedWith("Invalid arweave tx");
    });
    it("update rollup steps smoke test", async function () {
      const eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
      const hello = ethers.utils.formatBytes32String("hello");
      await expect(
        metaStore
          .connect(deployer)
          .registerDataNetwork(
            "https://rollup.com",
            deployer.address,
            ["https://index-node-1.com", "https://index-node-2.com"],
            [sender.address, deployer.address],
            hello
          )
      )
        .to.emit(eventLibABI, "CreateNetwork")
        .withArgs(deployer.address, 1);
      const binaryData = ethers.utils.base64.decode(
        "TY5SMaPPRk_TMvSDROaQWyc_WHyJrEL760-UhiNnHG4"
      );
      const arTx = ethers.utils.hexZeroPad(binaryData, 32);
      await expect(metaStore.connect(deployer).updateRollupSteps(1, arTx))
        .to.emit(eventLibABI, "UpdateRollupStep")
        .withArgs(deployer.address, 1, arTx);
    });
  });
});
