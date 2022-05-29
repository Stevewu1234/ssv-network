// File: contracts/SSVRegistry.sol
// SPDX-License-Identifier: GPL-3.0-or-later
pragma solidity ^0.8.2;

import "./ISSVRegistry.sol";

contract DKGRegistry {

    ISSVRegistry ssvRegistry;


    function DKGRegistry(){

    }


    function registerValidator(
        uint256[]  operatorIds,
        bytes[] signatures,
        bytes[] encryptedShares,
        bytes[] sharesPublicKeys
        uint8 setSize,
        bytes withdrawalCredentials,
        bytes publicKey,
    ) external {
         // verify signature
         for (uint8 index = 0; index < setSize; ++index) {

               address = ssvRegistry.operatorAddressByID(operatorIds[index]);

               require(
                    _verifySignature(
                        signatures[index],
                        address,
                        encryptedShares[index],
                        setSize,
                        withdrawalCredentials,
                        publicKey,
                    ),
               );
         }

         // register validator
        ssvRegistry.registerValidator(
            address(this),
            publicKey,
            operatorIds,
            sharesPublicKeys,
            encryptedShares,
            false,
        )
    }
}
