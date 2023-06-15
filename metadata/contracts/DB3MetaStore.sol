// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import {IMetaStore} from "./interfaces/IMetaStore.sol";

contract DB3MetaStore is IMetaStore {
    struct Network {
        address admin;
        address rollupNode;
        address[] indexNodes;
        bytes latestArTx;
    }
    mapping(bytes32 => Network) public networks;
    mapping(address => bytes32[]) public myNetworks;
}
