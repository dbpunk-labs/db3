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
                    ["https://index-node-1.com", "https://index-node-2.com"],
                    [sender.address, deployer.address]
                );

            const registration = await registry.getNetworkRegistration(1);

            expect(registration.rollupNodeUrl).to.equal("https://rollup-node.com");
            expect(registration.indexNodeUrls[0]).to.equal("https://index-node-1.com");
            expect(registration.indexNodeUrls[1]).to.equal("https://index-node-2.com");
            expect(registration.networkId).to.equal(1);
            expect(registration.sender).to.equal(deployer.address);
            expect(registration.rollupNodeAddress).to.equal(deployer.address);
            expect(registration.latestArweaveTx).to.equal("0x");
        });

        it("throws an error if Rollup node URL is empty", async function () {
            await expect(
                registry
                    .connect(deployer)
                    .registerNetwork(
                        1,
                        "",
                        deployer.address,
                        ["https://index-node-1.com", "https://index-node-2.com"],
                        [sender.address, deployer.address]
                    )
            ).to.be.revertedWith("Invalid Rollup node URL");
        });

        // it("throws an error if sender address is invalid", async function () {
        //     await expect(
        //         registry
        //             .connect(deployer)
        //             .registerNetwork(
        //                 1,
        //                 "https://rollup-node.com",
        //                 deployer.address,
        //                 ["https://index-node-1.com", "https://index-node-2.com"],
        //                 []
        //             )
        //     ).to.be.revertedWith("Invalid sender address");
        // });

        it("throws an error if Rollup node address is invalid", async function () {
            await expect(
                registry
                    .connect(deployer)
                    .registerNetwork(
                        1,
                        "https://rollup-node.com",
                        ethers.constants.AddressZero,
                        ["https://index-node-1.com", "https://index-node-2.com"],
                        []
                    )
            ).to.be.revertedWith("Invalid rollupNodeAddress address");
        });

        it("throws an error if network is already registered", async function () {
            await registry
                .connect(deployer)
                .registerNetwork(
                    1,
                    "https://rollup-node.com",
                    deployer.address,
                    ["https://index-node-1.com", "https://index-node-2.com"],
                    [sender.address, deployer.address]
                );

            await expect(
                registry
                    .connect(deployer)
                    .registerNetwork(
                        1,
                        "https://another-rollup-node.com",
                        deployer.address,
                        ["https://another-index-node.com"],
                        [deployer.address]
                    )
            ).to.be.revertedWith("Network already registered");
        });
    });

    describe("#registerRollupNode()", function () {
        it("updates Rollup node correctly", async function () {
            await registry
                .connect(deployer)
                .registerNetwork(
                    1,
                    "https://rollup-node.com",
                    deployer.address,
                    ["https://index-node-1.com"],
                    [sender.address]
                );

            await registry
                .connect(deployer)
                .registerRollupNode(1, "https://new-rollup-node.com");

            const registration = await registry.getNetworkRegistration(1);

            expect(registration.rollupNodeUrl).to.equal("https://new-rollup-node.com");
        });

        it("throws an error if network is not registered", async function () {
            await expect(
                registry
                    .connect(deployer)
                    .registerRollupNode(1, "https://new-rollup-node.com")
            ).to.be.revertedWith("Network not registered");
        });

        it("throws an error if sender is not the Rollup node address", async function () {
            await registry
                .connect(deployer)
                .registerNetwork(
                1,
                "https://rollup-node.com",
                deployer.address,
                ["https://index-node-1.com"],
                [sender.address]
                );
                await expect(
                    registry
                        .connect(sender)
                        .registerRollupNode(1, "https://new-rollup-node.com")
                ).to.be.revertedWith("msg.sender must be the same as RollupNodeAddress");
            });
        });
        describe("#registerIndexNode()", function () {
            it("adds Index node correctly", async function () {
                await registry
                    .connect(deployer)
                    .registerNetwork(
                        1,
                        "https://rollup-node.com",
                        deployer.address,
                        ["https://index-node-1.com"],
                        [sender.address]
                    );
        
                await registry
                    .connect(deployer)
                    .registerIndexNode(1, "https://index-node-2.com", sender.address);
        
                const registration = await registry.getNetworkRegistration(1);
        
                expect(registration.indexNodeUrls[1]).to.equal("https://index-node-2.com");
                expect(registration.indexNodeAddresses[1]).to.equal(sender.address);
            });
        
            it("throws an error if network is not registered", async function () {
                await expect(
                    registry
                        .connect(deployer)
                        .registerIndexNode(1, "https://index-node-2.com", sender.address)
                ).to.be.revertedWith("Network not registered");
            });
        
            it("throws an error if sender is not the Rollup node address", async function () {
                await registry
                    .connect(deployer)
                    .registerNetwork(
                        1,
                        "https://rollup-node.com",
                        deployer.address,
                        ["https://index-node-1.com"],
                        [sender.address]
                    );
        
                await expect(
                    registry
                        .connect(sender)
                        .registerIndexNode(1, "https://index-node-2.com", sender.address)
                ).to.be.revertedWith("msg.sender must be the same as RollupNodeAddress");
            });
        });
        
        describe("#updateRollupSteps()", function () {
            it("updates latest Arweave transaction correctly", async function () {
                await registry
                    .connect(deployer)
                    .registerNetwork(
                        1,
                        "https://rollup-node.com",
                        deployer.address,
                        ["https://index-node-1.com"],
                        [sender.address]
                    );
        
                const tx = "0x1234";
                await registry.connect(deployer).updateRollupSteps(1, tx);
        
                const registration = await registry.getNetworkRegistration(1);
        
                expect(registration.latestArweaveTx).to.equal(tx);
            });
        
            it("throws an error if network is not registered", async function () {
                const tx = "0x1234";
                await expect(
                    registry.connect(deployer).updateRollupSteps(1, tx)
                ).to.be.revertedWith("Network not registered");
            });
        
            it("throws an error if sender is not the Rollup node address", async function () {
                await registry
                    .connect(deployer)
                    .registerNetwork(
                        1,
                        "https://rollup-node.com",
                        deployer.address,
                        ["https://index-node-1.com"],
                        [sender.address]
                    );
        
                const tx = "0x1234";
                await expect(
                    registry.connect(sender).updateRollupSteps(1, tx)
                ).to.be.revertedWith("msg.sender must be the same as RollupNodeAddress");
            });
        });


        describe('#getNetworkRegistration()', function () {
            it('retrieves registration info for a specific network ID', async function () {
              await registry
                .connect(deployer)
                .registerNetwork(
                  1,
                  'https://rollup-node.com',
                  deployer.address,
                  ['https://index-node-1.com', 'https://index-node-2.com'],
                  [sender.address, deployer.address]
                );
        
              const registration = await registry.getNetworkRegistration(1);
        
              expect(registration.rollupNodeUrl).to.equal(
                'https://rollup-node.com'
              );
              expect(registration.indexNodeUrls[0]).to.equal(
                'https://index-node-1.com'
              );
              expect(registration.indexNodeUrls[1]).to.equal(
                'https://index-node-2.com'
              );
              expect(registration.networkId).to.equal(1);
              expect(registration.sender).to.equal(deployer.address);
              expect(registration.rollupNodeAddress).to.equal(deployer.address);
              expect(registration.latestArweaveTx).to.equal('0x');
            });
        
            it('throws an error if the specified network ID is not registered', async function () {
              await expect(registry.getNetworkRegistration(1)).to.be.revertedWith(
                'Network not registered'
              );
            });
          });
        
          describe('#getAllNetworkRegistrations()', function () {
            it('retrieves all registration info', async function () {
              await registry
                .connect(deployer)
                .registerNetwork(
                  1,
                  'https://rollup-node-1.com',
                  deployer.address,
                  ['https://index-node-1.com'],
                  [sender.address]
                );
        
              await registry
                .connect(deployer)
                .registerNetwork(
                  2,
                  'https://rollup-node-2.com',
                  sender.address,
                  ['https://index-node-2.com'],
                  [deployer.address]
                );
        
                        
              const pageSize = 2;
              const page = 1;

              const allRegistrations = await registry.getAllNetworkRegistrations(page,
                pageSize);
        
              expect(allRegistrations[0].rollupNodeUrl).to.equal(
                'https://rollup-node-1.com'
              );
              expect(allRegistrations[0].indexNodeUrls[0]).to.equal(
                'https://index-node-1.com'
              );
              expect(allRegistrations[0].networkId).to.equal(1);
              expect(allRegistrations[0].sender).to.equal(deployer.address);
              expect(allRegistrations[0].rollupNodeAddress).to.equal(deployer.address);
              expect(allRegistrations[0].latestArweaveTx).to.equal('0x');
        
              expect(allRegistrations[1].rollupNodeUrl).to.equal(
                'https://rollup-node-2.com'
              );
              expect(allRegistrations[1].indexNodeUrls[0]).to.equal(
                'https://index-node-2.com'
              );
              expect(allRegistrations[1].networkId).to.equal(2);
              expect(allRegistrations[1].sender).to.equal(deployer.address);
              expect(allRegistrations[1].rollupNodeAddress).to.equal(sender.address);
              expect(allRegistrations[1].latestArweaveTx).to.equal('0x');
            });
        
            it('retrieves specific registration info based on page size and number', async function () {
              await registry
                .connect(deployer)
                .registerNetwork(
                  1,
                  'https://rollup-node-1.com',
                  deployer.address,
                  ['https://index-node-1.com'],
                  [sender.address]
                );
        
              await registry
                .connect(deployer)
                .registerNetwork(
                  2,
                  'https://rollup-node-2.com',
                  sender.address,
                  ['https://index-node-2.com'],
                  [deployer.address]
                );
        
              const pageSize = 1;
              const page = 2;
        
              const pageRegistrations = await registry.getAllNetworkRegistrations(
                page,
                pageSize
              );
        
              expect(pageRegistrations.length).to.equal(1);
              expect(pageRegistrations[0].rollupNodeUrl).to.equal(
                'https://rollup-node-2.com'
              );
              expect(pageRegistrations[0].indexNodeUrls[0]).to.equal('https://index-node-2.com');
              expect(pageRegistrations[0].networkId).to.equal(2);
              expect(pageRegistrations[0].sender).to.equal(deployer.address);
              expect(pageRegistrations[0].rollupNodeAddress).to.equal(sender.address);
              expect(pageRegistrations[0].latestArweaveTx).to.equal('0x');
              });
              
            //   it('throws an error if the specified page number is greater than the total number of registrations', async function () {
            //     const pageSize = 1;
            //     const page = 1;
              
            //     await expect(
            //       registry.getAllNetworkRegistrations(page, pageSize)
            //     ).to.be.revertedWith('Network not registered');
            //   });

              });

        });
        