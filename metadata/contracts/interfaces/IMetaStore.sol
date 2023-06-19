// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

interface IMetaStore {

    ///
    /// Register a new data network
    ///
    function registerNetwork(
        bytes32 networkId,
        address sender,
        address rollupNode,
        address[] indexNodes
    ) external returns (bool);

    ///
    /// Register the rollup node url
    ///
    function registerRollupNode(
        uint64 networkId,
        string url
    ) external returns (bool);


    ///
    /// Update the rollup steps
    ///
    function updateRollupSteps(
        uint64 networkId,
        bytes arTx
    ) external returns (bool);

    ///
    /// Register the index node url
    /// @notice only the allowed indexer can register the node url
    function registerIndexNode(uint64 networkId, string url) external returns (bool);

    ///
    /// Get all networks registered by sender
    ///
    function getMyNetwork() external view return (bytes32[]);
}
