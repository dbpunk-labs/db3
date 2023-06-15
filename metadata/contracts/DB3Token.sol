// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";

///
///
/// @custom:security-contact dev@db3.network
///
///
contract Db3Token is ERC20 {
    /// the constructor for DB3 Token
    constructor() ERC20("db3 token", "db3") {
        // assign the sender 1000 DB3
        _mint(msg.sender, 10000 * 10 ** decimals());
    }

    /// the smallest unit is tai
    /// 1 db3 = 1000_000_000 tai
    function decimals() public pure override returns (uint8) {
        return 9;
    }
}
