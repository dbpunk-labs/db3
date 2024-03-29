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
  describe("registerDataNetwork", function () {
    it("registers a new network invalid rollup address", async function () {
      const hello = ethers.utils.formatBytes32String("hello");
      let eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
      await expect(
        metaStore
          .connect(deployer)
          .registerDataNetwork(
            "https://rollup.com",
            ethers.constants.AddressZero,
            ["https://index-node-1.com", "https://index-node-2.com"],
            [sender.address, deployer.address],
            hello
          )
      ).to.revertedWith("Invalid rollupNodeAddress address");
    });
    it("registers a new network invalid rollup node url ", async function () {
      const hello = ethers.utils.formatBytes32String("hello");
      let eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
      await expect(
        metaStore
          .connect(deployer)
          .registerDataNetwork(
            "",
            deployer.address,
            ["https://index-node-1.com", "https://index-node-2.com"],
            [sender.address, deployer.address],
            hello
          )
      ).to.revertedWith("Invalid Rollup node URL");
    });

    it("registers a lot of network ", async function () {
      const hello = ethers.utils.formatBytes32String("hello");
      const eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
      for (let i = 0; i < 100; i++) {
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
          .withArgs(deployer.address, i + 1);
      }
      const [dataNetworkCount, databaseCount, collectionCount] =
        await metaStore.getState();
      expect(dataNetworkCount).to.equal(100);
    });
    it("registers a new network smoke test ", async function () {
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
    });

    it("get network test", async function () {
      await expect(metaStore.getDataNetwork(2)).to.revertedWith(
        "Data Network is not registered"
      );
      let eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
      const hello = ethers.utils.formatBytes32String("hello");
      const empty = ethers.utils.formatBytes32String("");
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
      const dataNetwork = await metaStore.getDataNetwork(1);
      expect(deployer.address).to.equal(dataNetwork.admin);
      expect(deployer.address).to.equal(dataNetwork.rollupNodeAddress);
      expect(hello).to.equal(dataNetwork.description);
      expect(1).to.equal(dataNetwork.id);
      expect(0).to.equal(dataNetwork.latestRollupTime);
      expect(empty).to.equal(dataNetwork.latestArweaveTx);
      expect("https://rollup-node.com").to.equal(dataNetwork.rollupNodeUrl);
    });
  });
});
