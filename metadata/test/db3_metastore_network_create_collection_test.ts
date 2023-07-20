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
  describe("createCollection", function () {
    it("create collection invalid name", async function () {
      const eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
      const binaryData = ethers.utils.base64.decode(
        "TY5SMaPPRk_TMvSDROaQWyc_WHyJrEL760-UhiNnHG4"
      );
      const arTx = ethers.utils.hexZeroPad(binaryData, 32);
      const hello = ethers.utils.formatBytes32String("");
      const udl = ethers.utils.formatBytes32String("udl");
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
      await expect(
        metaStore
          .connect(deployer)
          .createCollection(
            1,
            "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
            hello,
            udl,
            arTx
          )
      ).to.revertedWith("name is empty");
    });

    it("create collection invalid license name", async function () {
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
      const udl = ethers.utils.formatBytes32String("");
      await expect(
        metaStore
          .connect(deployer)
          .createCollection(
            1,
            "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
            hello,
            udl,
            arTx
          )
      ).to.revertedWith("license is empty");
    });
    it("create collection invalid license content", async function () {
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
      const udl = ethers.utils.formatBytes32String("udl");
      const licenseContent = ethers.utils.formatBytes32String("");
      await expect(
        metaStore
          .connect(deployer)
          .createCollection(
            1,
            "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
            hello,
            udl,
            licenseContent
          )
      ).to.revertedWith("license content is empty");
    });

    it("create collection invalid network id", async function () {
      const binaryData = ethers.utils.base64.decode(
        "TY5SMaPPRk_TMvSDROaQWyc_WHyJrEL760-UhiNnHG4"
      );
      const arTx = ethers.utils.hexZeroPad(binaryData, 32);
      const hello = ethers.utils.formatBytes32String("hello");
      const udl = ethers.utils.formatBytes32String("udl");
      await expect(
        metaStore
          .connect(deployer)
          .createCollection(
            1,
            "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
            hello,
            udl,
            arTx
          )
      ).to.revertedWith("Data Network is not registered");
    });

    it("create collection invalid database", async function () {
      const eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
      const binaryData = ethers.utils.base64.decode(
        "TY5SMaPPRk_TMvSDROaQWyc_WHyJrEL760-UhiNnHG4"
      );
      const arTx = ethers.utils.hexZeroPad(binaryData, 32);
      const hello = ethers.utils.formatBytes32String("hello");
      const udl = ethers.utils.formatBytes32String("udl");
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
      await expect(
        metaStore
          .connect(deployer)
          .createCollection(
            1,
            "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
            hello,
            udl,
            arTx
          )
      ).to.revertedWith("Database was not found");
    });

    it("create collection invalid permission", async function () {
      const eventLibABI = await ethers.getContractAt(
        "Events",
        metaStore.address,
        deployer
      );
      const binaryData = ethers.utils.base64.decode(
        "TY5SMaPPRk_TMvSDROaQWyc_WHyJrEL760-UhiNnHG4"
      );
      const arTx = ethers.utils.hexZeroPad(binaryData, 32);
      const hello = ethers.utils.formatBytes32String("hello");
      const udl = ethers.utils.formatBytes32String("udl");
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
          "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
          hello
        );

      await expect(
        metaStore
          .connect(sender)
          .createCollection(
            1,
            "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
            hello,
            udl,
            arTx
          )
      ).to.revertedWith("You must the database sender");
    });

    it("create collection smoke test", async function () {
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
          "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
          hello
        );
      const binaryData = ethers.utils.base64.decode(
        "TY5SMaPPRk_TMvSDROaQWyc_WHyJrEL760-UhiNnHG4"
      );
      const arTx = ethers.utils.hexZeroPad(binaryData, 32);
      const udl = ethers.utils.formatBytes32String("udl");
      await expect(
        metaStore
          .connect(deployer)
          .createCollection(
            1,
            "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
            hello,
            udl,
            arTx
          )
      )
        .to.emit(eventLibABI, "CreateCollection")
        .withArgs(
          deployer.address,
          1,
          "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
          hello
        );
      const collection = await metaStore.getCollection(
        1,
        "0xF935E45C32C7DCc54bDDEcE5309c4313368A598A",
        hello
      );
      expect(collection.name).to.equal(hello);
      expect(collection.licenseName).to.equal(udl);
      expect(collection.licenseContent).to.equal(arTx);
      const [dataNetworkCount, databaseCount, collectionCount] =
        await metaStore.getState();
      expect(dataNetworkCount).to.equal(1);
      expect(databaseCount).to.equal(1);
      expect(collectionCount).to.equal(1);
    });
  });
});
