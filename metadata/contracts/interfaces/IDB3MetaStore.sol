// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "../Types.sol";

/**
 *
 * @title IDB3MetaStore
 * @author db3 network
 * the core metadata interface of db3 network to manage the global permanent database
 *
 */
interface IDB3MetaStore {
    /**
     * register a new data network and emit a create data network event with an id
     * @param rollupNodeUrl      The data rollup node rpc url
     * @param rollupNodeAddress  The evm address of data rollup node
     * @param indexNodeUrls      The urls of data index nodes
     * @param indexNodeAddresses The evm addresses of data index nodes
     */
    function registerDataNetwork(
        string memory rollupNodeUrl,
        address rollupNodeAddress,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses
    ) external;

    /**
     * update the data index node config
     * @param networkId          The id of your data network
     * @param indexNodeUrls      The urls of data index nodes
     * @param indexNodeAddresses The evm addresses of data index nodes
     */
    function updateIndexNodes(
        uint64 networkId,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses
    ) external;

    /**
     * get the data network by network id
     * @param id The id of your data network
     */
    function getDataNetwork(
        uint64 id
    ) external view returns (DataNetwork memory);

    /**
     * update the data rollup node config
     * @param id                  The id of your data network
     * @param rollupNodeUrls      The url of data rollup node
     * @param rollupNodeAddresses The evm addresses of data rollup node
     */
    function updateRollupNode(
        uint64 id,
        string memory rollupNodeUrl,
        address memory rollupNodeAddress
    ) external;

    /**
     * update the data rollup steps
     * @param id                  The id of your data network
     * @param latestArweaveTx     The latest arweave transaction id
     */
    function updateRollupSteps(
        uint64 id,
        bytes memory latestArweaveTx
    ) external;

    /**
     * create a document database
     * @param id                  The id of your data network
     */
    function createDocDatabase(uint64 id) external;
}
