// SPDX-License-Identifier: MIT
pragma solidity ^0.8.9;

import "@openzeppelin/contracts-upgradeable/token/ERC20/ERC20Upgradeable.sol";
import "@openzeppelin/contracts-upgradeable/token/ERC20/extensions/ERC20SnapshotUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/security/PausableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/Initializable.sol";

///
///
/// @custom:security-contact dev@db3.network
///
///
contract Db3Token is
    Initializable,
    ERC20Upgradeable,
    ERC20SnapshotUpgradeable,
    OwnableUpgradeable,
    PausableUpgradeable
{
    /// @custom:oz-upgrades-unsafe-allow constructor
    constructor() {
        _disableInitializers();
    }

    function initialize() public initializer {
        __ERC20_init("DB3 Network", "db3");
        __ERC20Snapshot_init();
        __Ownable_init();
        __Pausable_init();
        _totalSupply = 1000000000 * 10 ** decimals();
        _mint(msg.sender, 100000000 * 10 ** decimals());
    }

    /// the smallest unit is bit
    /// 1 db3 = 1000_000_000 bit
    function decimals() public pure override returns (uint8) {
        return 9;
    }

    function snapshot() public onlyOwner {
        _snapshot();
    }

    function pause() public onlyOwner {
        _pause();
    }

    function unpause() public onlyOwner {
        _unpause();
    }

    function mint(address to, uint256 amount) public onlyOwner {
        _mint(to, amount);
    }

    function _beforeTokenTransfer(
        address from,
        address to,
        uint256 amount
    )
        internal
        override(ERC20Upgradeable, ERC20SnapshotUpgradeable)
        whenNotPaused
    {
        super._beforeTokenTransfer(from, to, amount);
    }
}
