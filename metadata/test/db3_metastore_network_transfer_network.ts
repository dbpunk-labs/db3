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
  describe("transfer data network", function () {
    it("transfer an invalid data network", async function () {
      await expect(
        metaStore.connect(deployer).transferNetwork(1, deployer.address)
      ).to.revertedWith("Data Network is not registered");
    });
    it("transfer an invalid address", async function () {
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
            "https://rollup-node.com",
            deployer.address,
            ["https://index-node-1.com", "https://index-node-2.com"],
            [sender.address, deployer.address],
            hello
          )
      )
        .to.emit(eventLibABI, "CreateNetwork")
        .withArgs(deployer.address, 1);
      await expect(
        metaStore.connect(deployer).transferNetwork(1, deployer.address)
      ).to.revertedWith("you are transfering the data network to yourself");
    });
    it("transfer with no permission", async function () {
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
            "https://rollup-node.com",
            deployer.address,
            ["https://index-node-1.com", "https://index-node-2.com"],
            [sender.address, deployer.address],
            hello
          )
      )
        .to.emit(eventLibABI, "CreateNetwork")
        .withArgs(deployer.address, 1);
      await expect(
        metaStore.connect(sender).transferNetwork(1, deployer.address)
      ).to.revertedWith("msg.sender must be the same as data network admin");
    });
    it("transfer smoke test", async function () {
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
            "https://rollup-node.com",
            deployer.address,
            ["https://index-node-1.com", "https://index-node-2.com"],
            [sender.address, deployer.address],
            hello
          )
      )
        .to.emit(eventLibABI, "CreateNetwork")
        .withArgs(deployer.address, 1);
      await expect(
        metaStore.connect(deployer).transferNetwork(1, sender.address)
      )
        .to.emit(eventLibABI, "TransferNetwork")
        .withArgs(deployer.address, 1, sender.address);
      const dataNetwork = await metaStore.getDataNetwork(1);
      expect(sender.address).to.equal(dataNetwork.admin);
    });
  });
});
