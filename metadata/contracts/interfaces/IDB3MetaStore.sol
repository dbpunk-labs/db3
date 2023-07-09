// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import {Types} from "../libraries/Types.sol";

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
        address[] memory indexNodeAddresses,
        bytes32 description
    ) external;

    /**
     * update the data index node config
     * @param networkId          The id of your data network
     * @param indexNodeUrls      The urls of data index nodes
     * @param indexNodeAddresses The evm addresses of data index nodes
     */
    function updateIndexNodes(
        uint256 networkId,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses
    ) external;

    /**
     * get the data network by network id
     * @param id The id of your data network
     */
    function getDataNetwork(
        uint256 id
    ) external view returns (Types.DataNetwork memory);

    /**
     * update the data rollup node config
     * @param id                  The id of your data network
     * @param rollupNodeUrl       The url of data rollup node
     * @param rollupNodeAddress   The evm addresses of data rollup node
     */
    function updateRollupNode(
        uint256 id,
        string memory rollupNodeUrl,
        address rollupNodeAddress
    ) external;

    /**
     * update the data rollup steps
     * @param id                  The id of your data network
     * @param latestArweaveTx     The latest arweave transaction id
     */
    function updateRollupSteps(uint256 id, bytes32 latestArweaveTx) external;

    /**
     * create a document database
     * @param id                 The id of your data network
     * @param description        The description of database
     */
    function createDocDatabase(uint256 id, bytes32 description) external;

    /**
     * create a document collection
     * @param id                 The id of your data network
     * @param db                 The address of database
     * @param name               The name of collection
     * @param licenseName        The name of the license
     * @param licenseContent     The content is a arweave tx id
     */
    function createCollection(
        uint256 id,
        address db,
        bytes32 name,
        bytes32 licenseName,
        bytes32 licenseContent
    ) external;

    /**
     * get a document collection
     * @param id                 The id of your data network
     * @param db                 The address of database
     * @param name               The name of collection
     */
    function getCollection(
        uint256 id,
        address db,
        bytes32 name
    ) external view returns (Types.Collection memory);

    /**
     * transfer the data network to a new admin
     * @param id                 The id of your data network
     & @param to                 The address of new admin
     */
    function transferNetwork(uint256 id, address to) external;

    /**
     * transfer the data database to a maintainer
     * @param id                 The id of your data network
     * @param db                 The address of database
     & @param to                 The address of maintainer
     */
    function transferDatabase(uint256 id, address db, address to) external;

    /**
     * get the database
     * @param id                 The id of your data network
     * @param db                 The address of database
     */
    function getDatabase(
        uint256 id,
        address db
    ) external view returns (Types.Database memory);

    /**
     * get the state
     */
    function getState() external view returns (uint256, uint256, uint256);

    /**
     * fork a data network
     * @param id                 The id of your data network
     */
    function forkNetwork(uint256 id) external;
}
