import { ethers, upgrades } from 'hardhat';

const _crypto = require('crypto');
const Web3 = require('web3');
const OLD_CONTRACT_ABI = require('./old_abi.json');
const NEW_CONTRACT_ABI = require('./new_abi.json');
const SSV_TOKEN_ABI = require('./ssv_token.json');
const web3 = new Web3(process.env.MIGRATION_NODE_URL);

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

async function writeTx(contractAddress: any, privateKey: any, methodName: any, payload: any, value = 0) {
  const ssvToken = new web3.eth.Contract(SSV_TOKEN_ABI, process.env.SSVTOKEN_ADDRESS);

  const data = (await ssvToken.methods[methodName](...payload)).encodeABI();

  const transaction: any = {
    to: contractAddress,
    value,
    nonce: await web3.eth.getTransactionCount(web3.eth.accounts.privateKeyToAccount(privateKey).address, 'pending'),
    data
  };
  const gas = payload && await web3.eth.estimateGas({ ...transaction, from: web3.eth.accounts.privateKeyToAccount(privateKey).address }) * 2;

  transaction.gas = gas || 1500000;
  transaction.gasPrice = +await web3.eth.getGasPrice() * 10;

  console.log('tx request:', transaction);
  const signedTx = await web3.eth.accounts.signTransaction(transaction, privateKey);
  return new Promise<void>((resolve, reject) => {
    web3.eth.sendSignedTransaction(signedTx.rawTransaction, (error: any, hash: any) => {
      if (error) {
        console.log('❗Something went wrong while submitting your transaction:', error);
        reject();
      }
    })
    .on('transactionHash', (hash: any) => {
      console.log(`transaction hash is: ${hash}. in progress...`);
    })
    .on('receipt', (data: any) => {
      console.log('`🎉  got tx receipt');
      resolve();
    })
    .on('error', (error: any) => {
      console.log('tx error', error);
      reject();
    });
  });
}

async function main() {
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
    const found = newOperatorEvents.find((n: any) => n.returnValues.publicKey === oldValues.publicKey);
    if (!!!found) {
      console.log(index, oldValues.publicKey, !!found);
      const tx = await ssvNetwork.migrateRegisterOperator(
        oldValues.name,
        oldValues.ownerAddress,
        oldValues.publicKey,
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
    const found = newValidatorsEvents.find((n: any) => n.returnValues.publicKey === oldValues.publicKey);
    console.log(index, !!found);
    if (!!!found) {
      const usedOperatorIds = oldValues.oessList.map((rec: any) => +operatorIds[rec['operatorPublicKey']]);
      try {
        console.log('> deposit');
        const tokens = '150000000000000000000';
        await writeTx(process.env.SSVTOKEN_ADDRESS, process.env.GOERLI_OWNER_PRIVATE_KEY, 'approve', [process.env.MIGRATION_NEW_CONTRACT_ADDRESS, tokens]);
        const txDeposit = await ssvNetwork.migrationDeposit(oldValues.ownerAddress, tokens);
        await txDeposit.wait();
        console.log('> register');
        const tx = await ssvNetwork.migrateRegisterValidator(
          oldValues.ownerAddress,
          oldValues.publicKey,
          usedOperatorIds,
          oldValues.oessList.map((rec:any) => rec['sharedPublicKey']),
          oldValues.oessList.map((rec:any) => rec['encryptedKey']),
          0
        );
        await tx.wait();
        console.log(`${index}/${validatorsEvents.length}`, '+', oldValues.ownerAddress, oldValues.publicKey, operatorIds);  
      } catch (e) {
        console.log(e);
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