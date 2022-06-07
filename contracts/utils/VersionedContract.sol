pragma solidity ^0.8.4;

interface VersionedContract {
  function version() external pure returns (uint32);
}
