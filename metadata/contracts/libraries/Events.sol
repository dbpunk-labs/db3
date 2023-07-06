// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

library Events {
    /**
     * Emitted when a developer create a new network
     *
     * @param networkId The id of the network
     * @param sender    The sender of the  network
     * @param timestamp The current block timestamp.
     */
    event CreateNetwork(uint64 networkId, address sender, uint256 timestamp);

    /**
     * Emitted when a developer create a new database
     *
     * @param sender          The sender of transaction
     * @param networkId       The id of the network that the database belongs to
     * @param databaseAddress The generated database address
     */
    event CreateDatabase(
        address indexed sender,
        uint64 networkId,
        address databaseAddress
    );
}
