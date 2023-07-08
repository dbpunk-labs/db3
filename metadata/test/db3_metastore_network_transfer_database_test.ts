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
  describe("transfer database", function () {
    it("transfer an invalid data network", async function () {
      await expect(
        metaStore.connect(deployer).transferDatabase(1, "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A", sender.address)
      ).to.revertedWith("Data Network is not registered");
    });

    it("transfer without permission", async function () {
        const hello = ethers.utils.formatBytes32String("hello");
      const eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
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
      await expect(metaStore.connect(deployer).createDocDatabase(1, hello))
        .to.emit(eventLibABI, "CreateDatabase")
        .withArgs(
          deployer.address,
          1,
          "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A"
        );
      await expect(
        metaStore.connect(sender).transferDatabase(1, "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A", deployer.address)
      ).to.revertedWith("You must the database sender");
    });

    it("transfer database smoke test", async function () {
      const hello = ethers.utils.formatBytes32String("hello");
      const eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
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
      await expect(metaStore.connect(deployer).createDocDatabase(1, hello))
        .to.emit(eventLibABI, "CreateDatabase")
        .withArgs(
          deployer.address,
          1,
          "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A"
        );
      await expect(
        metaStore.connect(deployer).transferDatabase(1, "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A", sender.address)
      ).to.emit(eventLibABI, "TransferDatabase").withArgs(deployer.address, 1, "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A", sender.address)
    });
  });
});
