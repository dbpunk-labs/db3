// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

library Types {
    // the database basic information
    struct Database {
        // generate by the contract
        address db;
        // the mapping relationships
        mapping(bytes32 => bool) collecions;
        // the database sender can create collection
        address sender;
        // the description the database
        bytes32 description;
        // the counter of the collection id
        uint64 counter;
    }

    // the data network information
    struct DataNetwork {
        // the id of the network
        uint256 id;
        // the url of data rollup node
        string rollupNodeUrl;
        // the account address of data rollup node
        address rollupNodeAddress;
        // the urls of data index node
        string[] indexNodeUrls;
        // the address of data index node
        address[] indexNodeAddresses;
        // the admin who can add or change the node url
        address admin;
        // the latest arweave tx and the rollup node can update the latest arweave tx
        bytes32 latestArweaveTx;
        // the latest rollup time used track the network activty
        uint256 latestRollupTime;
        // the description of the data network
        bytes32 description;
    }
}
