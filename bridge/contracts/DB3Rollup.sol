// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "./DB3Token.sol";
import "@openzeppelin/contracts/utils/cryptography/MerkleProof.sol";
import "@openzeppelin/contracts/proxy/transparent/TransparentUpgradeableProxy.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";

contract DB3Rollup is Initializable {
    Db3Token private _tokenContract;
    // the locked balance of an address
    mapping(address => uint256) private _balances;
    bytes32 private _root;
    uint256 private _totalUnsignedFee;
    event Deposit(address from, uint256 amount);
    event Settlement(address owner, uint256 amount);

    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize(
        Db3Token tokenContract,
        bytes32 root
    ) public initializer {
        _tokenContract = tokenContract;
        _root = root;
    }

    function deposit(uint256 amount) public payable returns (bool) {
        _tokenContract.transferFrom(msg.sender, address(this), amount);
        _balances[msg.sender] += amount;
        emit Deposit(msg.sender, amount);
        return true;
    }

    function getLockedBalance(address owner) public view returns (uint256) {
        return _balances[owner];
    }

    function getTotalGasFee() public view returns (uint256) {
        return _totalUnsignedFee;
    }

    function _conversion(
        bytes[] calldata states
    ) private view returns (bytes32[] memory) {
        bytes32[] memory leaves = new bytes32[](states.length);
        for (uint i = 0; i < states.length; i++) {
            (address account, uint256 balance) = abi.decode(
                states[i],
                (address, uint256)
            );
            require(_balances[account] > balance);
            bytes32 leaf = keccak256(bytes.concat(keccak256(states[i])));
            leaves[i] = leaf;
        }
        return leaves;
    }

    function verifyStates(
        bytes32[] calldata proof,
        bool[] calldata proofFlags,
        bytes32 preRootHash,
        bytes32 postRootHash,
        bytes[] calldata states
    ) public view returns (bool) {
        require(preRootHash == _root);
        bytes32[] memory leaves = _conversion(states);
        return
            MerkleProof.multiProofVerifyCalldata(
                proof,
                proofFlags,
                postRootHash,
                leaves
            );
    }

    function processUpdateStates(
        bytes32[] calldata proof,
        bool[] calldata proofFlags,
        bytes32 preRootHash,
        bytes32 postRootHash,
        bytes[] calldata states
    ) public returns (bool) {
        require(preRootHash == _root);
        bytes32[] memory leaves = _conversion(states);
        require(
            MerkleProof.multiProofVerifyCalldata(
                proof,
                proofFlags,
                postRootHash,
                leaves
            )
        );
        for (uint i = 0; i < states.length; i++) {
            (address account, uint256 balance) = abi.decode(
                states[i],
                (address, uint256)
            );
            _totalUnsignedFee += _balances[account] - balance;
            _balances[account] = balance;
        }
        _root = postRootHash;
        return true;
    }
}
