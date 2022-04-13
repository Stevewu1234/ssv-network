import "./ISSVNetwork.sol";
import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts-upgradeable/access/OwnableUpgradeable.sol";

contract DKGRegistry is Initializable, OwnableUpgradeable {
    using ECDSA for bytes32;

    struct StakingDepositData {
        bytes publicKey;
        bytes withdrawalCredentials;
        bytes depositSignature;
        bytes depositDataRoot;
    }


    /**
     * @dev Emitted when a validator has been added.
     * @param publicKey is the validator's pubkey
     */
    event DKGValidatorAdded(bytes publicKey);

    ISSVNetwork private _ssvContract;
    mapping(uint256 => StakingDepositData) availableValidators;
    uint256 availableValidatorsCnt;
    uint256 availableValidatorsIdx;

    /**
        Registers a new available DKG validator
    */
    function addValidatorData(
        bytes calldata publicKey,
        uint256[] calldata operatorIds,
        bytes[] calldata sharesPublicKeys,
        bytes[] calldata encryptedKeys,
        bytes with_cred,
        bytes deposit_sig,
        bytes deposit_data_root,
        bytes[] calldata validationSigs
    ) external {

        // verify shares, shares pub keys and deposit credentials
        for (uint256 index = 0; index < operatorIds.length; ++index) {
            bytes32 root = keccak256(abi.encode(sharesPublicKeys[index], encryptedKeys[index], with_cred, deposit_sig, deposit_data_root));

            bytes sig = validationSigs[index];
            address _operatorAdd = _ssvContract.getOperatorAddress(operatorIds[index]);
            require(root.toEthSignedMessageHash().recover(sig) == _operatorAdd, "invalid shares sig");
        }

        // register validator (non-active)
        _ssvContract.registerValidator(
            publicKey,
            operatorIds,
            sharesPublicKeys,
            encryptedKeys,
            false
        );

        // add to availableValidators
        availableValidators[availableValidatorsCnt] = StakingDepositData(publicKey, with_cred, deposit_sig, deposit_data_root);
        ++availableValidatorsCnt;

        emit DKGValidatorAdded(publicKey);
    }

    /**
       Returns the next available validator and marks it as used
   */
    function getAvailableValidator() external returns (bytes publicKey, bytes with_cred, bytes deposit_sig,  bytes deposit_data_root) {
        require(availableValidatorsIdx <= availableValidatorsCnt, "no available validators");
        val = availableValidators[availableValidatorsIdx];
        // TODO - do we need to check something? someone else can use this validator somehow?
        // TODO - take fee?

        ++availableValidatorsIdx;

        return (val.publicKey, val.withdrawalCredentials, val.depositSignature, val.depositDataRoot);
    }
}