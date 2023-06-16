
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;
import "./interfaces/IDB3MetaStore.sol";

contract DB3MetaStore is IDB3MetaStore{

    // Mapping to store registration info for all registered networks
    mapping(uint64 => NetworkRegistration) private networkRegistrations;

    // Counter to keep track of number of registered networks
    uint256 private numNetworks;

    // Register a new network
    function registerNetwork(
        uint64 networkId,
        string memory rollupNodeUrl,
        string[] memory indexNodeUrls,
        bytes memory latestArweaveTx
    ) public {
        // Check if Rollup node address, Index node addresses and sender address are valid
        require(bytes(rollupNodeUrl).length > 0, "Invalid Rollup node URL");
       // require(indexNodeUrls.length > 0, "At least one Index node URL required");
        require(msg.sender != address(0), "Invalid sender address");

        // Check if network is already registered
        NetworkRegistration storage registration = networkRegistrations[networkId];
        require(bytes(registration.rollupNodeUrl).length == 0, "Network already registered");

        // Add new network info to struct and update mapping
        registration.rollupNodeUrl = rollupNodeUrl;
        registration.indexNodeUrls = indexNodeUrls;
        registration.networkId = networkId;
        registration.sender = msg.sender;
        registration.latestArweaveTx = latestArweaveTx;

        // Increment registered network counter
        numNetworks++;
    }

    // Get registration info for a specific network ID
    function getNetworkRegistration(uint64 networkId) public view returns (string memory rollupNodeUrl, string[] memory indexNodeUrls, uint64 registrationNetworkId, address sender, bytes memory latestArweaveTx) {
        // Get network registration struct and ensure it exists
        NetworkRegistration storage registration = networkRegistrations[networkId];
        require(bytes(registration.rollupNodeUrl).length > 0, "Network not registered");

        // Return registration info
        return (registration.rollupNodeUrl, registration.indexNodeUrls, registration.networkId, registration.sender, registration.latestArweaveTx);
    }

    // Get registration info for all networks (with pagination)
    function getAllNetworkRegistrations(uint64 page, uint64 pageSize) public view returns (NetworkRegistration[] memory registrations) {
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
            if (bytes(networkRegistrations[networkId].rollupNodeUrl).length > 0) {
                if ((i >= startIndex) && (i < endIndex)) {
                    registrations[i - startIndex] = networkRegistrations[networkId];
                }
                i++;
            }
        }

        // Return registration info array
        return registrations;
    }

    // Register a new Rollup node for a specific network ID
    function registerRollupNode(uint64 networkId, string memory rollupNodeUrl) public returns (bool success) {
        // Check if network is registered
        NetworkRegistration storage registration = networkRegistrations[networkId];
        require(bytes(registration.rollupNodeUrl).length > 0, "Network not registered");

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

        // Add new Index node URL to array in registration struct
        registration.indexNodeUrls.push(indexNodeUrl);
        return true;
    }

    
    // Update network information for a specific network ID
    function updateRollupSteps(uint64 networkId, bytes memory latestArweaveTx) public returns (bool success) {
        // Check if network is registered
        NetworkRegistration storage registration = networkRegistrations[networkId];
         require(
            bytes(registration.rollupNodeUrl).length > 0,
            "Network not registered"
        );

  // Update latest Arweave transaction in registration struct
        registration.latestArweaveTx = latestArweaveTx;
        return true;
    }
}

