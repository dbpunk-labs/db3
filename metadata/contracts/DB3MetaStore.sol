// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;
import "./interfaces/IDB3MetaStore.sol";

contract DB3MetaStore is IDB3MetaStore {
    // Mapping to store registration info for all registered networks
    mapping(uint64 => NetworkRegistration) private networkRegistrations;

    // Counter to keep track of number of registered networks
    uint256 private numNetworks;

    // Register a new network
    function registerNetwork(
        uint64 networkId,
        string memory rollupNodeUrl,
        address rollupNodeAddress,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses
    ) public {
        // Check if Rollup node address, Index node addresses and sender address are valid
        require(bytes(rollupNodeUrl).length > 0, "Invalid Rollup node URL");
        // require(indexNodeUrls.length > 0, "At least one Index node URL required");
        require(msg.sender != address(0), "Invalid sender address");
        require(
            rollupNodeAddress != address(0),
            "Invalid rollupNodeAddress address"
        );

        // Check if network is already registered
        NetworkRegistration storage registration = networkRegistrations[
            networkId
        ];
        require(
            bytes(registration.rollupNodeUrl).length == 0,
            "Network already registered"
        );

        // Add new network info to struct and update mapping
        registration.rollupNodeUrl = rollupNodeUrl;
        registration.indexNodeUrls = indexNodeUrls;
        registration.indexNodeAddresses = indexNodeAddresses;
        registration.networkId = networkId;
        registration.admin = msg.sender;
        registration.rollupNodeAddress = rollupNodeAddress;

        // Increment registered network counter
        numNetworks++;
    }

    // Update an existing network's information
    function updateNetworkIndexNodes(
        uint64 networkId,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses
    ) public {
        // Check if network is registered
        NetworkRegistration storage registration = networkRegistrations[
            networkId
        ];
        require(
            bytes(registration.rollupNodeUrl).length > 0,
            "Network not registered"
        );

        // Check if sender is the same as admin 
        require(
            msg.sender == registration.admin,
            "msg.sender must be the same as  registration.admin "
        );

        // Update  Index node URLs in registration struct

        if (indexNodeUrls.length > 0) {
            registration.indexNodeUrls = indexNodeUrls;
        }

        if (indexNodeAddresses.length > 0) {
            registration.indexNodeAddresses = indexNodeAddresses;
        }
    }

    // Get registration info for a specific network ID
    function getNetworkRegistration(
        uint64 networkId
    ) external view returns (NetworkRegistration memory registration) {
        // Get network registration struct and ensure it exists
        registration = networkRegistrations[networkId];
        require(
            bytes(registration.rollupNodeUrl).length > 0,
            "Network not registered"
        );

        // Return registration info
        return registration;
    }

    // Get registration info for all networks (with pagination)
    function getAllNetworkRegistrations(
        uint64 page,
        uint64 pageSize
    ) external view returns (NetworkRegistration[] memory registrations) {
        // Calculate number of registration infos to return
        uint256 startIndex = (page - 1) * pageSize;
        uint256 endIndex = startIndex + pageSize;
        if (endIndex > numNetworks) {
            endIndex = numNetworks;
        }
        uint256 numNetworksToReturn = endIndex - startIndex;

        // Create dynamic array to store registration infos
        registrations = new NetworkRegistration[](numNetworksToReturn);

        // Iterate over registered networks and add necessary registration infos to array
        uint256 i = 0;
        for (uint64 networkId = 1; networkId <= numNetworks; networkId++) {
            if (
                bytes(networkRegistrations[networkId].rollupNodeUrl).length > 0
            ) {
                if ((i >= startIndex) && (i < endIndex)) {
                    registrations[i - startIndex] = networkRegistrations[
                        networkId
                    ];
                }
                i++;
            }
        }

        // Return registration info array
        return registrations;
    }

    // Register a new Rollup node for a specific network ID
    function registerRollupNode(
        uint64 networkId,
        string memory rollupNodeUrl
    ) public returns (bool success) {
        // Check if rollupNodeUrl is not empty
        require(
            bytes(rollupNodeUrl).length > 0,
            "Rollup node URL cannot be empty"
        );

        // Check if network is registered
        NetworkRegistration storage registration = networkRegistrations[
            networkId
        ];

        require(
            bytes(registration.rollupNodeUrl).length > 0,
            "Network not registered"
        );

        // Check if sender is the same as rollupNodeAddress
        require(
            msg.sender == registration.rollupNodeAddress,
            "msg.sender must be the same as RollupNodeAddress"
        );

        // Update Rollup node in registration struct
        registration.rollupNodeUrl = rollupNodeUrl;
        return true;
    }

    // Register a new Index node for a specific network ID
    function registerIndexNode(
        uint64 networkId,
        string memory indexNodeUrl
    ) public returns (bool success) {
        // Check if network is registered
        NetworkRegistration storage registration = networkRegistrations[
            networkId
        ];
        require(
            bytes(registration.rollupNodeUrl).length > 0,
            "Network not registered"
        );

        // Check if sender is in the list of index node addresses
        bool senderIsRegistered = false;
        for (uint i = 0; i < registration.indexNodeAddresses.length; i++) {
            if (registration.indexNodeAddresses[i] == msg.sender) {
                senderIsRegistered = true;
                break;
            }
        }
        require(
            senderIsRegistered,
            "Sender address is not a registered Index node"
        );

        // Check if index node URL is not empty
        require(bytes(indexNodeUrl).length > 0, "Empty index node URL");

        // Update or add Index node URL to array in registration struct
        bool indexNodeUpdated = false;
        for (uint i = 0; i < registration.indexNodeUrls.length; i++) {
            if (registration.indexNodeAddresses[i] == msg.sender) {
                registration.indexNodeUrls[i] = indexNodeUrl;
                indexNodeUpdated = true;
                break;
            }
        }

        return indexNodeUpdated;
    }

    // Update network information for a specific network ID
    function updateRollupSteps(
        uint64 networkId,
        bytes memory latestArweaveTx
    ) public returns (bool success) {
        // Check if network is registered
        NetworkRegistration storage registration = networkRegistrations[
            networkId
        ];
        require(
            bytes(registration.rollupNodeUrl).length > 0,
            "Network not registered"
        );

        // Check if sender is the same as rollupNodeAddress
        require(
            msg.sender == registration.rollupNodeAddress,
            "msg.sender must be the same as RollupNodeAddress"
        );

        // Update latest Arweave transaction in registration struct
        registration.latestArweaveTx = latestArweaveTx;
        return true;
    }
}
