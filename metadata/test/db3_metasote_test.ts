import { expect } from "chai";
import { ethers } from "hardhat";
import { DB3MetaStore } from "../typechain-types";

describe("DB3MetaStore", function () {
  let registry: DB3MetaStore;
  let deployer: any;
  let sender: any;

  beforeEach(async function () {
    [deployer, sender] = await ethers.getSigners();
    const Registry = await ethers.getContractFactory("DB3MetaStore");
    registry = await Registry.deploy();
  });

  describe("#registerNetwork()", function () {
    it("registers a new network correctly", async function () {
      await registry
        .connect(deployer)
        .registerNetwork(
          1,
          "https://rollup-node.com",
          ["https://index-node-1.com", "https://index-node-2.com"],
          ethers.utils.toUtf8Bytes("latestArweaveTx")
        );

      const [rollupNodeUrl, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
        await registry.getNetworkRegistration(1);

      expect(rollupNodeUrl).to.equal("https://rollup-node.com");
      expect(indexNodeUrls[0]).to.equal("https://index-node-1.com");
      expect(indexNodeUrls[1]).to.equal("https://index-node-2.com");
      expect(networkId).to.equal(1);
      expect(senderAddress).to.equal(deployer.address);
      expect(ethers.utils.toUtf8String(latestArweaveTx)).to.equal("latestArweaveTx");
    });

    it("fails to register network with invalid Rollup node URL", async function () {
      await expect(
        registry.connect(deployer).registerNetwork(2, "", ["https://index-node.com"], ethers.utils.toUtf8Bytes("latestArweaveTx"))
      ).to.be.revertedWith("Invalid Rollup node URL");
    });


    it("fails to register already registered network", async function () {
      await registry
        .connect(deployer)
        .registerNetwork(
          1,
          "https://new-rollup-node.com",
          ["https://new-index-node.com"],
          ethers.utils.toUtf8Bytes("latestArweaveTx")
        );

      await expect(
        registry
          .connect(deployer)
          .registerNetwork(
            1,
            "https://new-rollup-node.com",
            ["https://new-index-node.com"],
            ethers.utils.toUtf8Bytes("latestArweaveTx")
          )
      ).to.be.revertedWith("Network already registered");
    });
  });

  describe("#getNetworkRegistration()", function () {
    it("returns correct registration info for a specific network ID", async function () {
      await registry
        .connect(deployer)
        .registerNetwork(
          1,
          "https://rollup-node.com",
          ["https://index-node-1.com", "https://index-node-2.com"],
          ethers.utils.toUtf8Bytes("latestArweaveTx")
        );

      const [rollupNodeUrl, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
        await registry.getNetworkRegistration(1);

      expect(rollupNodeUrl).to.equal("https://rollup-node.com");
      expect(indexNodeUrls[0]).to.equal("https://index-node-1.com");
      expect(indexNodeUrls[1]).to.equal("https://index-node-2.com");
      expect(networkId).to.equal(1);
      expect(senderAddress).to.equal(deployer.address);
      expect(ethers.utils.toUtf8String(latestArweaveTx)).to.equal("latestArweaveTx");
    });

    it("fails to get registration info for unregistered network ID", async function () {
      await expect(registry.getNetworkRegistration(2)).to.be.revertedWith(
        "Network not registered"
      );
    });
  });

  describe("#getAllNetworkRegistrations()", function () {
    it("returns correct registration info for all networks when only one page is needed", async function () {
      await registry
        .connect(deployer)
        .registerNetwork(1, "https://rollup-node.com", ["https://index-node-1.com"], ethers.utils.toUtf8Bytes("latestArweaveTx"));

      await registry
        .connect(deployer)
        .registerNetwork(2, "https://rollup-node-2.com", ["https://index-node-2.com"], ethers.utils.toUtf8Bytes("latestArweaveTx"));

      const registrations = await registry.getAllNetworkRegistrations(1, 10);

      expect(registrations.length).to.equal(2);
      expect(registrations[0].rollupNodeUrl).to.equal("https://rollup-node.com");
      expect(registrations[1].rollupNodeUrl).to.equal("https://rollup-node-2.com");
    });

    it("returns correct registration info for all networks when multiple pages are needed", async function () {
      for (let i = 1; i <= 15; i++) {
        await registry
          .connect(deployer)
          .registerNetwork(i, `https://rollup-node-${i}.com`, [`https://index-node-${i}.com`], ethers.utils.toUtf8Bytes("latestArweaveTx"));
      }

      const firstPage = await registry.getAllNetworkRegistrations(1, 10);
      expect(firstPage.length).to.equal(10);
      expect(firstPage[0].rollupNodeUrl).to.equal("https://rollup-node-1.com");
      expect(firstPage[9].rollupNodeUrl).to.equal("https://rollup-node-10.com");

      const secondPage = await registry.getAllNetworkRegistrations(2, 10);
      expect(secondPage.length).to.equal(5);
      expect(secondPage[0].rollupNodeUrl).to.equal("https://rollup-node-11.com");
      expect(secondPage[4].rollupNodeUrl).to.equal("https://rollup-node-15.com");
    });
  });


  describe("#registerRollupNode()", function () {
    it("registers a new Rollup node correctly", async function () {
      await registry.connect(deployer).registerNetwork(1, "https://rollup-node.com", [], ethers.utils.toUtf8Bytes("latestArweaveTx"));

      await registry.connect(deployer).registerRollupNode(1, "https://new-rollup-node.com");

      const [rollupNodeUrl, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
        await registry.getNetworkRegistration(1);

      expect(rollupNodeUrl).to.equal("https://new-rollup-node.com");
      expect(indexNodeUrls.length).to.equal(0);
      expect(networkId).to.equal(1);
      expect(senderAddress).to.equal(deployer.address);
      expect(ethers.utils.toUtf8String(latestArweaveTx)).to.equal("latestArweaveTx");
    });

    it("fails to register Rollup node for unregistered network ID", async function () {
      await expect(
        registry.connect(deployer).registerRollupNode(2, "https://new-rollup-node.com")
      ).to.be.revertedWith("Network not registered");
    });
  });

  describe("#registerIndexNode()", function () {
    it("registers a new Index node correctly", async function () {
      await registry.connect(deployer).registerNetwork(1, "https://rollup-node.com", [], ethers.utils.toUtf8Bytes("latestArweaveTx"));

      await registry.connect(deployer).registerIndexNode(1, "https://new-index-node.com");

      const [rollupNodeUrl, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
        await registry.getNetworkRegistration(1);

      expect(rollupNodeUrl).to.equal("https://rollup-node.com");
      expect(indexNodeUrls[0]).to.equal("https://new-index-node.com");
      expect(networkId).to.equal(1);
      expect(senderAddress).to.equal(deployer.address);
      expect(ethers.utils.toUtf8String(latestArweaveTx)).to.equal("latestArweaveTx");
    });

    it("fails to register Index node for unregistered network ID", async function () {
      await expect(
        registry.connect(deployer).registerIndexNode(2, "https://new-index-node.com")
      ).to.be.revertedWith("Network not registered");
    });
  });

  describe("#updateRollupSteps()", function () {
    it("updates existing network correctly", async function () {
      await registry
        .connect(deployer)
        .registerNetwork(
          1,
          "https://old-rollup-node.com",
          ["https://old-index-node.com"],
          ethers.utils.toUtf8Bytes("oldArweaveTx")
        );
      await registry
        .connect(deployer)
        .updateRollupSteps(
          1,
          ethers.utils.toUtf8Bytes("newArweaveTx")
        );

      const [rollupNodeUrl, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
        await registry.getNetworkRegistration(1);

      expect(networkId).to.equal(1);
      expect(senderAddress).to.equal(deployer.address);
      expect(ethers.utils.toUtf8String(latestArweaveTx)).to.equal("newArweaveTx");
    });

    // it("fails to update network with invalid Rollup node URL", async function () {
    //   await registry
    //     .connect(deployer)
    //     .registerNetwork(
    //       1,
    //       "https://old-rollup-node.com",
    //       ["https://old-index-node.com"],
    //       ethers.utils.toUtf8Bytes("oldArweaveTx")
    //     );

    //   await expect(
    //     registry.connect(deployer).updateRollupSteps(1, ethers.utils.toUtf8Bytes("newArweaveTx"))
    //   ).to.be.revertedWith("Invalid Rollup node URL");
    // });

    it("fails to update unregistered network", async function () {
      await expect(
        registry.connect(deployer).updateRollupSteps(1,  ethers.utils.toUtf8Bytes("newArweaveTx"))
      ).to.be.revertedWith("Network not registered");
    });
  });
});

