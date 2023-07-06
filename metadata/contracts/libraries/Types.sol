// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

library DataTypes {
    // the database basic information
    struct Database {
        // generate by the contract
        address db;
        // the mapping relationships
        mapping(string => string) collecions;
        // the database sender can create collection
        address sender;
    }

    // the data network information
    struct DataNetwork {
        // the id of the network
        uint64 id;
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
        bytes latestArweaveTx;
        // the latest rollup time used track the network activty
        uint64 latestRollupTime;
        // the all database
        mapping(address => Database) databases;
        // the description of the data network
        string description;
    }
}
