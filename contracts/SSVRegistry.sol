// File: contracts/SSVRegistry.sol
// SPDX-License-Identifier: GPL-3.0-or-later
pragma solidity ^0.8.2;

import "@openzeppelin/contracts/utils/math/Math.sol";
import "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";
import "./ISSVRegistry.sol";
import "hardhat/console.sol";

contract SSVRegistry is Initializable, OwnableUpgradeable, ISSVRegistry {
    uint256 public operatorCount;
    uint256 public validatorCount;

    mapping(bytes => Operator) public override operators;
    mapping(bytes => Validator) public override validators;

    mapping(bytes => OperatorFee[]) private operatorFees;

    mapping(address => bytes[]) private operatorsByAddress;
    mapping(address => bytes[]) private validatorsByAddress;

    function initialize() public virtual override initializer {
        __SSVRegistry_init();
    }

    function __SSVRegistry_init() internal initializer {
        __Ownable_init_unchained();
        __SSVRegistry_init_unchained();
    }

    function __SSVRegistry_init_unchained() internal initializer {
    }

    function getValidatorOwner(bytes calldata _publicKey) onlyOwner external override view returns (address) {
        return validators[_publicKey].ownerAddress;
    }

    function getOperatorOwner(bytes calldata _publicKey) onlyOwner external override view returns (address) {
        return operators[_publicKey].ownerAddress;
    }

    function _validateValidatorParams(
        bytes calldata _publicKey,
        bytes[] calldata _operatorPublicKeys,
        bytes[] calldata _sharesPublicKeys,
        bytes[] calldata _encryptedKeys
    ) private pure {
        require(_publicKey.length == 48, "invalid public key length");
        require(
            _operatorPublicKeys.length == _sharesPublicKeys.length &&
                _operatorPublicKeys.length == _encryptedKeys.length,
            "OESS data structure is not valid"
        );
    }

    /**
     * @dev See {ISSVRegistry-registerOperator}.
     */
    function registerOperator(
        string calldata _name,
        address _ownerAddress,
        bytes calldata _publicKey,
        uint256 _fee
    ) onlyOwner public virtual override {
        require(
            operators[_publicKey].ownerAddress == address(0),
            "operator with same public key already exists"
        );
        operators[_publicKey] = Operator(_name, _ownerAddress, _publicKey, 0, false, operatorsByAddress[_ownerAddress].length);
        operatorsByAddress[_ownerAddress].push(_publicKey);
        emit OperatorAdded(_name, _ownerAddress, _publicKey);
        operatorCount++;
        updateOperatorFee(_publicKey, _fee);
        activateOperator(_publicKey);
    }

    /**
     * @dev See {ISSVRegistry-registerValidator}.
     */
    function registerValidator(
        address _ownerAddress,
        bytes calldata _publicKey,
        bytes[] calldata _operatorPublicKeys,
        bytes[] calldata _sharesPublicKeys,
        bytes[] calldata _encryptedKeys
    ) onlyOwner public virtual override {
        _validateValidatorParams(
            _publicKey,
            _operatorPublicKeys,
            _sharesPublicKeys,
            _encryptedKeys
        );
        require(_ownerAddress != address(0), "owner address invalid");
        require(
            validators[_publicKey].ownerAddress == address(0),
            "validator with same public key already exists"
        );

        Validator storage validatorItem = validators[_publicKey];
        validatorItem.publicKey = _publicKey;
        validatorItem.ownerAddress = _ownerAddress;

        for (uint256 index = 0; index < _operatorPublicKeys.length; ++index) {
            validatorItem.oess.push(
                Oess(
                    index,
                    _operatorPublicKeys[index],
                    _sharesPublicKeys[index],
                    _encryptedKeys[index]
                )
            );
        }
        validatorItem.index = validatorsByAddress[_ownerAddress].length;
        validatorsByAddress[_ownerAddress].push(_publicKey);
        validatorCount++;
        emit ValidatorAdded(_ownerAddress, _publicKey, validatorItem.oess);
        activateValidator(_publicKey);
    }

    /**
     * @dev See {ISSVRegistry-updateValidator}.
     */
    function updateValidator(
        bytes calldata _publicKey,
        bytes[] calldata _operatorPublicKeys,
        bytes[] calldata _sharesPublicKeys,
        bytes[] calldata _encryptedKeys
    ) onlyOwner public virtual override {
        _validateValidatorParams(
            _publicKey,
            _operatorPublicKeys,
            _sharesPublicKeys,
            _encryptedKeys
        );
        Validator storage validatorItem = validators[_publicKey];
        delete validatorItem.oess;

        for (uint256 index = 0; index < _operatorPublicKeys.length; ++index) {
            validatorItem.oess.push(
                Oess(
                    index,
                    _operatorPublicKeys[index],
                    _sharesPublicKeys[index],
                    _encryptedKeys[index]
                )
            );
        }

        emit ValidatorUpdated(validatorItem.ownerAddress, _publicKey, validatorItem.oess);
    }

    /**
     * @dev See {ISSVRegistry-deleteValidator}.
     */
    function deleteValidator(
        address _ownerAddress,
        bytes calldata _publicKey
    ) onlyOwner public virtual override {
        Validator storage validatorItem = validators[_publicKey];
        validatorsByAddress[_ownerAddress][validatorItem.index] = validatorsByAddress[_ownerAddress][validatorsByAddress[_ownerAddress].length - 1];
        validators[validatorsByAddress[_ownerAddress][validatorItem.index]].index = validatorItem.index;
        validatorsByAddress[_ownerAddress].pop();
        delete validators[_publicKey];
        --validatorCount;
        emit ValidatorDeleted(_ownerAddress, _publicKey);
    }

    /**
     * @dev See {ISSVRegistry-deleteOperator}.
     */
    function deleteOperator(
        address _ownerAddress,
        bytes calldata _publicKey
    ) onlyOwner public virtual override {
        Operator storage operatorItem = operators[_publicKey];
        string memory name = operatorItem.name;
        operatorsByAddress[_ownerAddress][operatorItem.index] = operatorsByAddress[_ownerAddress][operatorsByAddress[_ownerAddress].length - 1];
        operators[operatorsByAddress[_ownerAddress][operatorItem.index]].index = operatorItem.index;
        operatorsByAddress[_ownerAddress].pop();
        delete operators[_publicKey];

        --operatorCount;

        emit OperatorDeleted(name, _publicKey);
    }

    /**
     * @dev See {ISSVRegistry-getOperatorCurrentFee}.
     */
    function getOperatorCurrentFee(bytes calldata _operatorPubKey) public view override returns (uint256) {
        require(operatorFees[_operatorPubKey].length > 0, "operator not found");
        return operatorFees[_operatorPubKey][operatorFees[_operatorPubKey].length - 1].fee;
    }

    /**
     * @dev See {ISSVRegistry-getValidatorUsage}.
     */
    function getValidatorUsage(
        bytes calldata _pubKey,
        uint256 _fromBlockNumber,
        uint256 _toBlockNumber
    ) onlyOwner public view override returns (uint256 usage) {
        for (uint256 index = 0; index < validators[_pubKey].oess.length; ++index) {
            Oess memory oessItem = validators[_pubKey].oess[index];
            uint256 lastBlockNumber = _toBlockNumber;
            bool oldestFeeUsed = false;
            for (uint256 feeReverseIndex = 0; !oldestFeeUsed && feeReverseIndex < operatorFees[oessItem.operatorPublicKey].length; ++feeReverseIndex) {
                uint256 feeIndex = operatorFees[oessItem.operatorPublicKey].length - feeReverseIndex - 1;
                if (operatorFees[oessItem.operatorPublicKey][feeIndex].blockNumber < lastBlockNumber) {
                    uint256 startBlockNumber = Math.max(_fromBlockNumber, operatorFees[oessItem.operatorPublicKey][feeIndex].blockNumber);
                    usage += (lastBlockNumber - startBlockNumber) * operatorFees[oessItem.operatorPublicKey][feeIndex].fee;
                    if (startBlockNumber == _fromBlockNumber) {
                        oldestFeeUsed = true;
                    } else {
                        lastBlockNumber = startBlockNumber;
                    }
                }
            }
        }
    }

    /**
     * @dev See {ISSVRegistry-updateOperatorFee}.
     */
    function updateOperatorFee(bytes calldata _pubKey, uint256 _fee) onlyOwner public virtual override {
        operatorFees[_pubKey].push(
            OperatorFee(block.number, _fee)
        );
        emit OperatorFeeUpdated(_pubKey, block.number, _fee);
    }

    function getOperatorPubKeysInUse(bytes calldata _validatorPubKey) onlyOwner public virtual override returns (bytes[] memory operatorPubKeys) {
        Validator storage validatorItem = validators[_validatorPubKey];

        operatorPubKeys = new bytes[](validatorItem.oess.length);
        for (uint256 index = 0; index < validatorItem.oess.length; ++index) {
            operatorPubKeys[index] = validatorItem.oess[index].operatorPublicKey;
        }
    }

    function getOperatorsByAddress(address _ownerAddress) onlyOwner external view virtual override returns (bytes[] memory) {
        return operatorsByAddress[_ownerAddress];
    }

    function getValidatorsByAddress(address _ownerAddress) onlyOwner external view virtual override returns (bytes[] memory) {
        return validatorsByAddress[_ownerAddress];
    }

    function activateOperator(bytes calldata _pubKey) onlyOwner override public {
        require(!operators[_pubKey].active, "already active");
        operators[_pubKey].active = true;

        emit OperatorActive(operators[_pubKey].ownerAddress, _pubKey);
    }

    function deactivateOperator(bytes calldata _pubKey) onlyOwner override external {
        require(operators[_pubKey].active, "already inactive");
        operators[_pubKey].active = false;

        emit OperatorInactive(operators[_pubKey].ownerAddress, _pubKey);
    }

    function activateValidator(bytes calldata _pubKey) onlyOwner override public {
        require(!validators[_pubKey].active, "already active");
        validators[_pubKey].active = true;

        emit ValidatorActive(validators[_pubKey].ownerAddress, _pubKey);
    }

    function deactivateValidator(bytes calldata _pubKey) onlyOwner override external {
        require(validators[_pubKey].active, "already inactive");
        validators[_pubKey].active = false;

        emit ValidatorInactive(validators[_pubKey].ownerAddress, _pubKey);
    }
}
