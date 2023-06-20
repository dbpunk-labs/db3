// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

interface IDB3MetaStore {
    struct NetworkRegistration {
        uint64 networkId;
        string rollupNodeUrl;
        string[] indexNodeUrls;
        address[] indexNodeAddresses;
        address admin;
        address rollupNodeAddress;
        bytes latestArweaveTx;
    }

    function registerNetwork(
        uint64 networkId,
        string memory rollupNodeUrl,
        address rollupNodeAddress,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses
    ) external;

    function updateNetworkIndexNodes(
        uint64 networkId,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses
    ) external;

    function getNetworkRegistration(
        uint64 networkId
    ) external view returns (NetworkRegistration memory);

    function getAllNetworkRegistrations(
        uint64 page,
        uint64 pageSize
    ) external view returns (NetworkRegistration[] memory);

    function registerRollupNode(
        uint64 networkId,
        string memory rollupNodeUrl
    ) external returns (bool success);

    function registerIndexNode(
        uint64 networkId,
        string memory indexNodeUrl
    ) external returns (bool success);

    function updateRollupSteps(
        uint64 networkId,
        bytes memory latestArweaveTx
    ) external returns (bool success);
}
