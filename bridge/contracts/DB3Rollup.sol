// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "./DB3Token.sol";

contract DB3Rollup {
    Db3Token private _tokenContract;
    // the locked balance of an address
    mapping(address => uint256) private _balances;

    constructor(Db3Token tokenContract) {
        _tokenContract = tokenContract;
    }

    function deposit(uint256 amount) public returns (bool) {
        _tokenContract.transfer(address(this), amount);
        _balances[msg.sender] += amount;
        return true;
    }

    function getLockedBalance() public view returns (uint256) {
        return _balances[msg.sender];
    }
}
