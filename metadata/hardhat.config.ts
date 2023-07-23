import { HardhatUserConfig } from "hardhat/config";
import "@nomicfoundation/hardhat-toolbox";
//import "@matterlabs/hardhat-zksync-deploy";
//import "@matterlabs/hardhat-zksync-solc";

const config: HardhatUserConfig = {
  solidity: "0.8.17",
  zksolc: {
    version: "latest",
    settings: {},
  },
  //defaultNetwork: "localhost",
  networks: {
    lineaTest: {
      url: "https://linea-goerli.infura.io/v3/1ff2ead2c89442d290c2b99ec01cbab8" || "",
      accounts:
        process.env.PRIVATE_KEY !== undefined ? [process.env.PRIVATE_KEY] : [],
      zksync: false,
    },
    zkSyncTest: {
        url:"https://zksync2-testnet.zksync.dev" || "",
        accounts: process.env.PRIVATE_KEY !== undefined ? [process.env.PRIVATE_KEY] : [],
        ethNetwork: "goerli",
        zksync: true

    },
    mumbai: {
      url: "https://polygon-mumbai.infura.io/v3/4458cf4d1689497b9a38b1d6bbf05e78" || "",
      zksync: false,
      accounts:
        process.env.PRIVATE_KEY !== undefined ? [process.env.PRIVATE_KEY] : [],
    },
    scrollalpha: {
      url: "https://alpha-rpc.scroll.io/l2" || "",
      zksync: false,
      accounts:
        process.env.PRIVATE_KEY !== undefined ? [process.env.PRIVATE_KEY] : [],
    },
    localhost: {
      url: "http://127.0.0.1:8545" || "",
      zksync: false,
      accounts:
        process.env.PRIVATE_KEY !== undefined ? [process.env.PRIVATE_KEY] : [],
    },
  },
  artifacts:"./artifacts"
};

export default config;
