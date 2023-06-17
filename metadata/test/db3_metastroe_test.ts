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
                    deployer.address,
                    ["https://index-node-1.com", "https://index-node-2.com"]
                );

            const [rollupNodeUrl, rollupNodeAddress, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
                await registry.getNetworkRegistration(1);

            expect(rollupNodeUrl).to.equal("https://rollup-node.com");
            expect(indexNodeUrls[0]).to.equal("https://index-node-1.com");
            expect(indexNodeUrls[1]).to.equal("https://index-node-2.com");
            expect(networkId).to.equal(1);
            expect(senderAddress).to.equal(deployer.address);
            expect(rollupNodeAddress).to.equal(deployer.address);
            expect(latestArweaveTx).to.equal("0x");
        });

        it("fails to register network with invalid Rollup node URL", async function () {
            await expect(
                registry.connect(deployer).registerNetwork(
                    2,
                    "",
                    deployer.address,
                    ["https://index-node.com"]
                )
            ).to.be.revertedWith("Invalid Rollup node URL");
        });

        it("fails to register network with invalid Rollup node address", async function () {
            await expect(
                registry.connect(deployer).registerNetwork(
                    3,
                    "https://rollup-node.com",
                    ethers.constants.AddressZero,
                    ["https://index-node.com"]
                )
            ).to.be.revertedWith("Invalid rollupNodeAddress address");
        });

        it("fails to register already registered network", async function () {
            await registry
                .connect(deployer)
                .registerNetwork(
                    1,
                    "https://new-rollup-node.com",
                    deployer.address,
                    ["https://new-index-node.com"],
                );

            await expect(
                registry
                    .connect(deployer)
                    .registerNetwork(
                        1,
                        "https://new-rollup-node.com",
                        deployer.address,
                        ["https://new-index-node.com"],
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
                    deployer.address,
                    ["https://index-node-1.com", "https://index-node-2.com"],
                );

            const [rollupNodeUrl, rollupNodeAddress, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
                await registry.getNetworkRegistration(1);

            expect(rollupNodeUrl).to.equal("https://rollup-node.com");
            expect(indexNodeUrls[0]).to.equal("https://index-node-1.com");
            expect(indexNodeUrls[1]).to.equal("https://index-node-2.com");
            expect(networkId).to.equal(1);
            expect(senderAddress).to.equal(deployer.address);
            expect(rollupNodeAddress).to.equal(deployer.address);
            expect(latestArweaveTx).to.equal("0x");
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
                .registerNetwork(
                    1,
                    "https://rollup-node.com",
                    deployer.address,
                    ["https://index-node-1.com"],
                );

            await registry
                .connect(deployer)
                .registerNetwork(
                    2,
                    "https://new-rollup-node.com",
                    deployer.address,
                    ["https://new-index-node.com"],
                );

            const [registration1, registration2] = await registry.getAllNetworkRegistrations(
                1,
                2
            );

            expect(registration1.rollupNodeUrl).to.equal("https://rollup-node.com");
            expect(registration1.indexNodeUrls[0]).to.equal("https://index-node-1.com");
            expect(registration1.networkId).to.equal(1);
            expect(registration1.sender).to.equal(deployer.address);
            expect(registration1.rollupNodeAddress).to.equal(deployer.address);
            expect(registration1.latestArweaveTx).to.equal("0x");

            expect(registration2.rollupNodeUrl).to.equal("https://new-rollup-node.com");
            expect(registration2.indexNodeUrls[0]).to.equal("https://new-index-node.com");
            expect(registration2.networkId).to.equal(2);
            expect(registration2.sender).to.equal(deployer.address);
            expect(registration2.rollupNodeAddress).to.equal(deployer.address);
            expect(registration2.latestArweaveTx).to.equal("0x");
        });

        it("returns correct registration info for all networks when multiple pages are needed", async function () {
            // Register 10 networks
            for (let i = 1; i <= 20; i++) {
                await registry
                    .connect(deployer)
                    .registerNetwork(
                        i,
                        `https://rollup-node-${i}.com`,
                        deployer.address,
                        [`https://index-node-${i}-1.com`, `https://index-node-${i}-2.com`]
                    );
            }

            // Get first page of 3 registration infos
            const registrationsPage1 = await registry.getAllNetworkRegistrations(1, 3);

            expect(registrationsPage1.length).to.equal(3);
            expect(registrationsPage1[0].networkId).to.equal(1);
            expect(registrationsPage1[1].networkId).to.equal(2);
            expect(registrationsPage1[2].networkId).to.equal(3);

            // Get second page of 3 registration infos
            const registrationsPage2 = await registry.getAllNetworkRegistrations(2, 3);

            expect(registrationsPage2.length).to.equal(3);
            expect(registrationsPage2[0].networkId).to.equal(4);
            expect(registrationsPage2[1].networkId).to.equal(5);
            expect(registrationsPage2[2].networkId).to.equal(6);

            // Get third page of 4 registration infos
            const registrationsPage3 = await registry.getAllNetworkRegistrations(3, 4);
            console.log(registrationsPage3);
            expect(registrationsPage3.length).to.equal(4);
            expect(registrationsPage3[0].networkId).to.equal(9);
            expect(registrationsPage3[1].networkId).to.equal(10);
            expect(registrationsPage3[2].networkId).to.equal(11);
            expect(registrationsPage3[3].networkId).to.equal(12);
        });
    });
    describe("#registerRollupNode()", function () {
        it("updates Rollup node for registered network correctly", async function () {
            await registry
                .connect(deployer)
                .registerNetwork(
                    1,
                    "https://old-rollup-node.com",
                    deployer.address,
                    ["https://index-node.com"],
                );

            await registry
                .connect(deployer)
                .registerRollupNode(1, "https://new-rollup-node.com", sender.address);

            const [rollupNodeUrl, rollupNodeAddress, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
                await registry.getNetworkRegistration(1);

            expect(rollupNodeUrl).to.equal("https://new-rollup-node.com");
            expect(indexNodeUrls[0]).to.equal("https://index-node.com");
            expect(networkId).to.equal(1);
            expect(senderAddress).to.equal(deployer.address);
            expect(rollupNodeAddress).to.equal(sender.address);
            expect(latestArweaveTx).to.equal("0x");
        });

        it("fails to update Rollup node for unregistered network", async function () {
            await expect(
                registry
                    .connect(deployer)
                    .registerRollupNode(2, "https://rollup-node.com", deployer.address)
            ).to.be.revertedWith("Network not registered");
        });
    });

    describe("#registerIndexNode()", function () {
        it("adds Index node for registered network correctly", async function () {
            await registry
                .connect(deployer)
                .registerNetwork(
                    1,
                    "https://rollup-node.com",
                    deployer.address,
                    ["https://index-node-1.com"],
                );

            await registry.connect(deployer).registerIndexNode(1, "https://index-node-2.com");

            const [rollupNodeUrl, rollupNodeAddress, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
                await registry.getNetworkRegistration(1);

            expect(indexNodeUrls[0]).to.equal("https://index-node-1.com");
            expect(indexNodeUrls[1]).to.equal("https://index-node-2.com");
        });

        it("fails to add Index node for unregistered network", async function () {
            await expect(
                registry.connect(deployer).registerIndexNode(2, "https://index-node.com")
            ).to.be.revertedWith("Network not registered");
        });
    });

    describe("#updateRollupSteps()", function () {
        it("updates the latest Arweave transaction hash correctly for registered network", async function () {
            await registry
                .connect(deployer)
                .registerNetwork(
                    1,
                    "https://rollup-node.com",
                    deployer.address,
                    ["https://index-node.com"],
                );

            await registry
                .connect(deployer)
                .updateRollupSteps(1, "0x1234567890123456789012345678901234567890123456789012345678901234");

            const [rollupNodeUrl, rollupNodeAddress, indexNodeUrls, networkId, senderAddress, latestArweaveTx] =
                await registry.getNetworkRegistration(1);

            expect(latestArweaveTx).to.equal("0x1234567890123456789012345678901234567890123456789012345678901234");
        });

        it("fails to update latest Arweave transaction hash for unregistered network", async function () {
            await expect(
                registry
                    .connect(deployer)
                    .updateRollupSteps(2, "0x1234567890123456789012345678901234567890123456789012345678901234")
            ).to.be.revertedWith("Network not registered");
        });
    });
});