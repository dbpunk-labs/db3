// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;
import {IDB3MetaStore} from "./interfaces/IDB3MetaStore.sol";
import {Types} from "./libraries/Types.sol";
import {Events} from "./libraries/Events.sol";

contract DB3MetaStore is IDB3MetaStore {
    // A map to store data network information
    mapping(uint256 => Types.DataNetwork) private _dataNetworks;
    // A map to store database information
    mapping(uint256 => mapping(address => Types.Database)) private _databases;
    // A map to store collection information
    mapping(uint256 => mapping(address => mapping(bytes32 => Types.Collection)))
        private _collections;
    // Counter to keep track of number of data networks
    uint256 private _networkCounter;
    // Counter to keep track of number of database
    uint256 private _databaseCounter;
    // Counter to keep track of number of collection
    uint256 private _collectionCounter;

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
        dataNetwork.latestArweaveTx = bytes32(0);
        dataNetwork.latestRollupTime = 0;
        // emit a create network event
        emit Events.CreateNetwork(msg.sender, _networkCounter);
    }

    function updateIndexNodes(
        uint256 networkId,
        string[] memory indexNodeUrls,
        address[] memory indexNodeAddresses
    ) public {
        require(networkId != 0, "invalid data network");
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
        require(networkId != 0, "invalid data network");
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
        require(networkId != 0, "invalid data network");
        // Check the data network must be registered
        require(networkId <= _networkCounter, "Data Network is not registered");
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
        require(networkId != 0, "invalid data network");
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        // Check the latestarweavetx
        require(latestArweaveTx != bytes32(0), "Invalid arweave tx");
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
        require(networkId != 0, "invalid data network");
        require(networkId <= _networkCounter, "Data Network is not registered");
        // Everyone can create a database currently
        _databaseCounter++;
        address db = address(
            uint160(
                bytes20(
                    keccak256(
                        abi.encodePacked(
                            networkId,
                            _databaseCounter,
                            msg.sender
                        )
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
        emit Events.CreateDatabase(msg.sender, networkId, db, description);
    }

    function createCollection(
        uint256 networkId,
        address db,
        bytes32 name,
        bytes32 licenseName,
        bytes32 licenseContent
    ) public {
        require(networkId != 0, "invalid data network");
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        require(name != bytes32(0), "name is empty");
        require(licenseName != bytes32(0), "license is empty");
        require(licenseContent != bytes32(0), "license content is empty");
        // Everyone can create a database currently
        Types.Database storage database = _databases[networkId][db];
        require(database.db != address(0), "Database was not found");
        // Check the permission
        require(database.sender == msg.sender, "You must the database sender");
        Types.Collection storage collection = _collections[networkId][db][name];
        // The collection name must not be used
        require(
            collection.created == false,
            "The collection name has been used"
        );
        _collectionCounter++;
        collection.created = true;
        collection.name = name;
        collection.licenseName = licenseName;
        collection.licenseContent = licenseContent;
        emit Events.CreateCollection(msg.sender, networkId, db, name);
    }

    function getCollection(
        uint256 networkId,
        address db,
        bytes32 name
    ) public view returns (Types.Collection memory collection) {
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        // Everyone can create a database currently
        Types.Database storage database = _databases[networkId][db];
        require(database.db != address(0), "Database was not found");
        collection = _collections[networkId][db][name];
        // The collection name must not be used
        require(collection.created == true, "The collection was not found");
        return collection;
    }

    function transferNetwork(uint256 networkId, address to) public {
        require(networkId != 0, "invalid data network");
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        require(
            msg.sender != to,
            "you are transfering the data network to yourself"
        );
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
        require(networkId != 0, "invalid data network");
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        Types.Database storage database = _databases[networkId][db];
        require(database.sender == msg.sender, "You must the database sender");
        database.sender = to;
        emit Events.TransferDatabase(msg.sender, networkId, db, to);
    }

    function getDatabase(
        uint256 id,
        address db
    ) public view returns (Types.Database memory database) {
        // Check if network is registered
        require(id <= _networkCounter, "Data Network is not registered");
        database = _databases[id][db];
        require(database.sender != address(0), "the must be a exist database");
        return database;
    }

    function getState() public view returns (uint256, uint256, uint256) {
        return (_networkCounter, _databaseCounter, _collectionCounter);
    }

    function forkNetwork(uint256 networkId) public {
        require(networkId != 0, "invalid data network");
        // Check if network is registered
        require(networkId <= _networkCounter, "Data Network is not registered");
        Types.DataNetwork storage dataNetwork = _dataNetworks[_networkCounter];
        _networkCounter++;
        Types.DataNetwork storage forkedDataNetwork = _dataNetworks[
            _networkCounter
        ];
        forkedDataNetwork.admin = msg.sender;
        forkedDataNetwork.id = _networkCounter;
        forkedDataNetwork.description = dataNetwork.description;
        forkedDataNetwork.latestArweaveTx = dataNetwork.latestArweaveTx;
        forkedDataNetwork.latestRollupTime = dataNetwork.latestRollupTime;
        emit Events.ForkNetwork(msg.sender, networkId, _networkCounter);
    }
}
