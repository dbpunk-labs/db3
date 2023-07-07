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
        it("registers a new network ", async function () {
            const hello = ethers.utils.formatBytes32String("hello");
            let eventLibABI = await ethers.getContractAt("Events", metaStore.address, deployer)

            await expect(metaStore
                .connect(deployer)
                .registerDataNetwork(
                    "https://rollup-node.com",
                    deployer.address,
                    ["https://index-node-1.com", "https://index-node-2.com"],
                    [sender.address, deployer.address],
                    hello
                )).to.emit(eventLibABI, "CreateNetwork").withArgs(deployer.address, 1);
            expect(await metaStore.getDataNetworkAdmin(1)).to.equal(deployer.address);
        });
    });
});
