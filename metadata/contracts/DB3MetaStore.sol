// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;
import {IDB3MetaStore} from "./interfaces/IDB3MetaStore.sol";
import {Types} from "./libraries/Types.sol";
import {Events} from "./libraries/Events.sol";

contract DB3MetaStore is IDB3MetaStore {
    // A map to store all data network information
    mapping(uint256 => Types.DataNetwork) private _dataNetworks;
    mapping(uint256 => mapping(address=> Types.Database)) private _databases;
    mapping(uint256 => mapping(address=> mapping(bytes32=>bool))) private _collections;
    // Counter to keep track of number of data networks
    uint256 private _networkCounter;
    // Counter to keep track of number of database
    uint256 private _databaseCounter;

    function registerDataNetwork(
        string memory rollupNodeUrl,
        address rollupNodeAddress,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses,
        bytes32 description
    ) public {
        // Check if Rollup node address, Index node addresses and sender address are valid
        require(bytes(rollupNodeUrl).length > 0, "Invalid Rollup node URL");
        require(msg.sender != address(0), "Invalid sender address");
        require(
            rollupNodeAddress != address(0),
            "Invalid rollupNodeAddress address"
        );
        _networkCounter++;
        Types.DataNetwork storage dataNetwork = _dataNetworks[_networkCounter];
        dataNetwork.id = _networkCounter;
        dataNetwork.rollupNodeUrl = rollupNodeUrl;
        dataNetwork.rollupNodeAddress = rollupNodeAddress;
        dataNetwork.admin = msg.sender;
        dataNetwork.indexNodeUrls = indexNodeUrls;
        dataNetwork.indexNodeAddresses = indexNodeAddresses;
        dataNetwork.description = description;
        // emit a create network event
        emit Events.CreateNetwork(msg.sender, _networkCounter);
    }

    function updateIndexNodes(
        uint256 networkId,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses
    ) public {
        // Check the network must be registered
        require(networkId <= _networkCounter, "Network is not registered");
        Types.DataNetwork storage dataNetwork = _dataNetworks[networkId];
        // Check permission
        require(msg.sender == dataNetwork.admin, "you are not the admin");
        dataNetwork.indexNodeUrls = indexNodeUrls;
        dataNetwork.indexNodeAddresses = indexNodeAddresses;
        emit Events.UpdateIndexNode(
            msg.sender,
            networkId,
            indexNodeAddresses,
            indexNodeUrls
        );
    }

    function getDataNetwork(
        uint256 networkId
    ) external view returns (Types.DataNetwork memory dataNetwork) {
        // Check the data network must be registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        // Get data network struct
        dataNetwork = _dataNetworks[networkId];
        return dataNetwork;
    }


    // Register a new Rollup node for a specific network ID
    function updateRollupNode(
        uint256 networkId,
        string memory rollupNodeUrl,
        address rollupNodeAddress
    ) public {
        // Check the data network must be registered
        require(networkId <= _networkCounter, "Data Network is not registered");

        // Check if rollupNodeUrl is not empty
        require(
            bytes(rollupNodeUrl).length > 0,
            "Rollup node URL cannot be empty"
        );

        // Check if network is registered
        Types.DataNetwork storage dataNetwork = _dataNetworks[networkId];
        // check the permission
        require(msg.sender == dataNetwork.admin, "you are not the admin");
        // Update Rollup node url
        dataNetwork.rollupNodeUrl = rollupNodeUrl;
        // We allow disable the rollup node by setting the address to 0x0
        dataNetwork.rollupNodeAddress = rollupNodeAddress;
        emit Events.UpdateRollupNode(
            msg.sender,
            networkId,
            rollupNodeAddress,
            rollupNodeUrl
        );
    }

    // Update network information for a specific network ID
    function updateRollupSteps(
        uint256 networkId,
        bytes32 latestArweaveTx
    ) public {
        // Check the latestarweavetx
        require(latestArweaveTx != bytes32(0), "Invalid arweave tx");
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");

        Types.DataNetwork storage dataNetwork = _dataNetworks[networkId];

        // Check the rollup permission
        require(
            msg.sender == dataNetwork.rollupNodeAddress,
            "msg.sender must be the same as RollupNodeAddress"
        );
        // Update latest Arweave transaction in registration struct
        dataNetwork.latestArweaveTx = latestArweaveTx;
        dataNetwork.latestRollupTime = block.timestamp;
        // emit an event
        emit Events.UpdateRollupStep(msg.sender, networkId, latestArweaveTx);
    }

    function createDocDatabase(uint256 networkId, bytes32 description) public {
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        // Everyone can create a database currently
        _databaseCounter++;
        address db = address(
            uint160(
                bytes20(
                    keccak256(
                        abi.encodePacked(networkId, _databaseCounter, msg.sender)
                    )
                )
            )
        );
        Types.Database storage database = _databases[networkId][db];
        require(database.sender == address(0), "the must be a new database");
        database.sender = msg.sender;
        database.db = db;
        database.description = description;
        database.counter = 0;
        emit Events.CreateDatabase(msg.sender, networkId, db);
    }

    function createCollection(
        uint256 networkId,
        address db,
        bytes32 name
    ) public {
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        // Everyone can create a database currently
        Types.Database storage database = _databases[networkId][db];
        // Check the permission
        require(database.sender == msg.sender, "You must the database sender");
        bool created = _collections[networkId][db][name];
        // The collection name must not be used
        require(created == false, "The collection name has been used");
        emit Events.CreateCollection(msg.sender, networkId, db, name);
    }

    function transferNetwork(uint256 networkId, address to) public {
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        Types.DataNetwork storage dataNetwork = _dataNetworks[networkId];
        // Check the transfer permission
        require(
            msg.sender == dataNetwork.admin,
            "msg.sender must be the same as data network admin"
        );
        dataNetwork.admin = to;
        emit Events.TransferNetwork(msg.sender, networkId, to);
    }

    function transferDatabase(
        uint256 networkId,
        address db,
        address to
    ) public {
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        Types.Database storage database = _databases[networkId][db];
        require(database.sender == msg.sender, "You must the database sender");
        database.sender = to;
        emit Events.TransferDatabase(msg.sender, networkId, db, to);
    }

    function getDatabase(uint256 id, address db) public view returns (Types.Database memory database) {
        // Check if network is registered
        require(id <= _networkCounter, "Data Network is not registered");
        database = _databases[id][db];
        require(database.sender == address(0), "the must be a new database");
        return database;
    }

}
