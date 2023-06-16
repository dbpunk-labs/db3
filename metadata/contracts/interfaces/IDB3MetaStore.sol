// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

interface IDB3MetaStore {
    struct NetworkRegistration {
        string rollupNodeUrl;
        string[] indexNodeUrls;
        uint64 networkId;
        address sender;
        bytes latestArweaveTx;
    }

    function registerNetwork(
        uint64 networkId,
        string memory rollupNodeUrl,
        string[] memory indexNodeUrls,
        bytes memory latestArweaveTx
    ) external;

    function getNetworkRegistration(uint64 networkId) external view returns (
        string memory rollupNodeUrl,
        string[] memory indexNodeUrls,
        uint64 registrationNetworkId,
        address sender,
        bytes memory latestArweaveTx
    );

    function getAllNetworkRegistrations(uint64 page, uint64 pageSize) external view returns (NetworkRegistration[] memory);

    function registerRollupNode(uint64 networkId, string memory rollupNodeUrl) external returns (bool success);

    function registerIndexNode(uint64 networkId, string memory indexNodeUrl) external returns (bool success);
    
    function updateRollupSteps(uint64 networkId, bytes memory latestArweaveTx) external returns (bool success);
}
