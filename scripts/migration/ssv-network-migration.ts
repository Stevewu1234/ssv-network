import { ethers, upgrades } from 'hardhat';

const _crypto = require('crypto');
const Web3 = require('web3');
const fs = require('fs');
const OLD_CONTRACT_ABI = require('./old_abi.json');
const NEW_CONTRACT_ABI = require('./new_abi.json');

function toChunks(items, size) {
  return Array.from(
    new Array(Math.ceil(items.length / size)),
    (_, i) => items.slice(i * size, i * size + size)
  );
}

function convertPublickey(rawValue) {
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
  const ssvNetwork = await ssvNetworkFactory.attach(process.env.MIGRATION_NEW_CONTRACT_ADDRESS);
  /*
  console.log(`fetching operators...`, filters);
  const operatorEvents = await oldContract.getPastEvents('OperatorAdded', filters);
  console.log("total operatorEvents", operatorEvents.length);
  let total = 0;
  let params = [[],[],[],[]];
  for (let index = 3; index < operatorEvents.length; index++) {
    const { returnValues } = operatorEvents[index];
    if (total === 3) {
      const tx = await ssvNetwork.batchRegisterOperator(
        params[0],
        params[1],
        params[2],
        params[3]
      );
      await tx.wait();
      params[0].forEach((value, idx) => console.log('+', params[0][idx], params[1][idx], params[2][idx], params[3][idx]));
      total = 0;
      params = [[],[],[],[]];
    }
    params[0].push(returnValues.name);
    params[1].push(returnValues.ownerAddress);
    params[2].push(returnValues.publicKey);
    params[3].push(0);
    total++;
  }
  if (total > 0) {
    try {
      const tx = await ssvNetwork.batchRegisterOperator(
        params[0],
        params[1],
        params[2],
        params[3]
      );
      await tx.wait();
      params[0].forEach((value, idx) => console.log('+', params[0][idx], params[1][idx], params[2][idx], params[3][idx]));  
    } catch (e) {
      console.log('------', params[0], e.message);
    }
  }
  return;
  */
  const latestBlock = await web3.eth.getBlockNumber();
  let fromBlock = 0;
  let toBlock = 0;
  let allEvents = [];
  do {
    toBlock += 300000;
    const filters = {
      fromBlock,
      toBlock
    };
    console.log(`fetching validators...`, filters);
    const validatorEvents = await oldContract.getPastEvents('ValidatorAdded', filters);
    console.log("total validatorEvents", validatorEvents.length);
    allEvents = [...allEvents, ...validatorEvents];
    fromBlock = toBlock + 1;
  } while (toBlock < latestBlock)
  const output = {};
  for (let index = 0; index < allEvents.length; index++) {
    const { returnValues: { oessList } } = allEvents[index];
    oessList.forEach(obj => {
      output[obj.operatorPublicKey] = output[obj.operatorPublicKey] || { eventsBased: 0, counterBased: 0 };
      output[obj.operatorPublicKey].eventsBased++;
    });
  }
  fs.appendFileSync('output.csv','pubKey,eventsBased,counterBased\n\r');
  for (const pubKey of Object.keys(output)) {
    output[pubKey].counterBased = await oldContract.methods.validatorsPerOperatorCount(pubKey).call();
    fs.appendFileSync('output.csv',`${pubKey},${output[pubKey].eventsBased},${output[pubKey].counterBased}\n`);
  }
  for (const pubKey of Object.keys(output)) {

  }
}

main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error(error);
    process.exit(1);
  });
