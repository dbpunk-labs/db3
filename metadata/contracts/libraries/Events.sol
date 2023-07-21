// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

library Events {
    /**
     * Emitted when a developer create a new network
     *
     * @param networkId The id of the network
     * @param sender    The sender of the  network
     */
    event CreateNetwork(address indexed sender, uint256 networkId);

    /**
     * Emitted when a developer transfer the data network to a new admin
     *
     * @param networkId The id of the network
     * @param sender    The sender of the  network
     */
    event TransferNetwork(
        address indexed sender,
        uint256 networkId,
        address to
    );

    /**
     * Emitted when a developer transfer the data network to a new admin
     *
     * @param sender    The sender of the  network
     * @param networkId The id of the network
     * @param db        The address of the database
     * @param to        The address of receiver
     */
    event TransferDatabase(
        address indexed sender,
        uint256 networkId,
        address db,
        address to
    );

    /**
     * Emitted when a developer create a new database
     *
     * @param sender          The sender of transaction
     * @param networkId       The id of the network that the database belongs to
     * @param databaseAddress The generated database address
     */
    event CreateDatabase(
        address indexed sender,
        uint256 networkId,
        address databaseAddress,
        bytes32 desc
    );

    /**
     * Emitted when a developer create a new collection
     *
     * @param sender          The sender of transaction
     * @param networkId       The id of the network that the database belongs to
     * @param arweaveTx       The transaction id of arweave
     */
    event UpdateRollupStep(
        address sender,
        uint256 networkId,
        bytes32 arweaveTx
    );

    /**
     * Emitted when a developer update the rollup node config
     *
     * @param sender            The sender of transaction
     * @param networkId         The id of the network that the database belongs to
     * @param rollupNodeAddress The evm address of rollup node
     * @param rollupNodeUrl     The url of the rollup node
     */
    event UpdateRollupNode(
        address sender,
        uint256 networkId,
        address rollupNodeAddress,
        string rollupNodeUrl
    );

    /**
     * Emitted when a developer update the rollup node config
     *
     * @param sender             The sender of transaction
     * @param networkId          The id of the network that the database belongs to
     * @param indexNodeAddresses The evm address of index node
     * @param indexNodeUrls      The urls of the index node
     */
    event UpdateIndexNode(
        address sender,
        uint256 networkId,
        address[] indexNodeAddresses,
        string[] indexNodeUrls
    );

    /**
     * Emitted when a developer create a new collection
     *
     * @param sender          The sender of transaction
     * @param networkId       The id of the network that the database belongs to
     * @param db              The address of database
     * @param name            The name of collection
     */
    event CreateCollection(
        address sender,
        uint256 networkId,
        address db,
        bytes32 name
    );

    /**
     * Emitted when a developer fork a network
     *
     * @param sender          The sender of transaction
     * @param networkId       The id of the network
     */
    event ForkNetwork(
        address sender,
        uint256 networkId,
        uint256 forkedNetworkId
    );
}
