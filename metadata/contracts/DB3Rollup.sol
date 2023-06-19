// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "./DB3Token.sol";

contract DB3Rollup {
    Db3Token private _tokenContract;
    // the locked balance of an address
    mapping(address => uint256) private _balances;
    event Deposit(address _from, uint256 amount);

    constructor(Db3Token tokenContract) {
        _tokenContract = tokenContract;
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
}
