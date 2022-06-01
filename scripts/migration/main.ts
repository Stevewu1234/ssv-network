import { ethers, upgrades } from 'hardhat';

const _crypto = require('crypto');
const Web3 = require('web3');
const OLD_CONTRACT_ABI = require('./old_abi.json');
const NEW_CONTRACT_ABI = require('./new_abi.json');

function toChunks(items: any, size: number) {
  return Array.from(
    new Array(Math.ceil(items.length / size)),
    (_, i) => items.slice(i * size, i * size + size)
  );
}

function convertPublickey(rawValue: string) {
  try {
    const decoded = (new Web3()).eth.abi.decodeParameter('string', rawValue.replace('0x', ''));
    return _crypto.createHash('sha256').update(decoded).digest('hex');
  } catch (e) {
    console.warn('PUBKEY WAS NOT CONVERTED DUE TO ', e);
    return rawValue;
  }
}

async function main() {
  const web3 = new Web3(process.env.MIGRATION_NODE_URL);
  const oldContract = new web3.eth.Contract(OLD_CONTRACT_ABI, process.env.MIGRATION_OLD_CONTRACT_ADDRESS);
  const newContract = new web3.eth.Contract(NEW_CONTRACT_ABI, process.env.MIGRATION_NEW_CONTRACT_ADDRESS);

  const ssvNetworkFactory = await ethers.getContractFactory('SSVNetwork');
  const ssvNetwork = await ssvNetworkFactory.attach(process.env.MIGRATION_NEW_CONTRACT_ADDRESS || '');
  const latestBlock = await web3.eth.getBlockNumber();
  const filters = {
    fromBlock: 0,
    toBlock: latestBlock
  };
  console.log(`fetching operators...`, filters);
  const operatorEvents = await oldContract.getPastEvents('OperatorAdded', filters);
  console.log("total operatorEvents", operatorEvents.length);

  let newOperatorEvents = await newContract.getPastEvents('OperatorAdded', filters);
  console.log("total new operatorEvents", newOperatorEvents.length);

  for (let index = 0; index < operatorEvents.length; index++) {
    const { returnValues: oldValues } = operatorEvents[index];
    const found = newOperatorEvents.find((n: any) => n.returnValues.publicKey === oldValues.publicKey && n.returnValues.ownerAddress === oldValues.ownerAddress);
    console.log(index, !!found);
    if (!!!found) {
      const tx = await ssvNetwork.migrateRegisterOperator(
        found.name,
        found.ownerAddress,
        found.publicKey,
        0
      );
      await tx.wait();
    }
  }

  console.log('resync new operators...');
  newOperatorEvents = await newContract.getPastEvents('OperatorAdded', filters);
  console.log("total new operatorEvents", newOperatorEvents.length);
  const operatorIds: any = {};
  for (let index = 0; index < newOperatorEvents.length; index++) {
    const { returnValues } = newOperatorEvents[index];
    operatorIds[returnValues.publicKey] = returnValues.id;
  }

  console.log(`fetching validators...`, filters);
  const validatorsEvents = await oldContract.getPastEvents('ValidatorAdded', filters);
  const newValidatorsEvents = await newContract.getPastEvents('ValidatorAdded', filters);
  console.log("total old validatorEvents", validatorsEvents.length);
  console.log("total new validatorEvents", newValidatorsEvents.length);

  for (let index = 0; index < validatorsEvents.length; index++) {
    const { returnValues: oldValues } = validatorsEvents[index];
    const found = newValidatorsEvents.find((n: any) => n.returnValues.publicKey === oldValues.publicKey && n.returnValues.ownerAddress === oldValues.ownerAddress);
    console.log(index, !!found);
    if (!!!found) {
      const operatorIds = oldValues.operatorPublicKeys.map((key: any) => operatorIds[key]);
      try {
        const tx = await ssvNetwork.migrateRegisterValidator(
          oldValues.ownerAddress,
          oldValues.publicKey,
          operatorIds,
          oldValues.sharesPublicKeys,
          oldValues.encryptedKeys,
          0
        );
        await tx.wait();
        console.log(`${index}/${validatorsEvents.length}`, '+', oldValues.ownerAddress, oldValues.publicKey, operatorIds);  
      } catch (e) {
        console.log(`${index}/${validatorsEvents.length}`, '------', oldValues.publicKey);
      }  
    }
  }
}

main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error(error);
    process.exit(1);
  });