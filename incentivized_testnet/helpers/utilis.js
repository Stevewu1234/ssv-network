const got = require('got');
const Web3 = require('web3');
function uniq(a) {
    return [...new Set(a)];
}

const web3 = new Web3(process.env.INFURA_URL);
const CONTRACT_ABI = require('../vestedContractAbi.json');
const fs = require("fs");
const contract = new web3.eth.Contract(CONTRACT_ABI, '0xB8471180C79A0a69C7790A1CCf62e91b3c3559Bf');

const hashedOperators = {};
const hashedValidators = {};
const delay = timeToWait => new Promise(resolve => setTimeout(resolve, timeToWait));

function sliceIntoChunks(arr, chunkSize) {
    const res = [];
    for (let i = 0; i < arr.length; i += chunkSize) {
        const chunk = arr.slice(i, i + chunkSize);
        res.push(chunk);
    }
    return res;
}

async function processOperators(fromEpoch, toEpoch) {
    let counter = 1;
    const operators = await getOperators();
    const batches = sliceIntoChunks(operators, Number(process.env.RATE_LIMIT));

    for (const batch of batches) {
        const processedOperators = batch.map(async (operator) => {
            if (operator.owner_address !== '0x943a1b677da0ac80f380f08731fae841b1201402') {
                const performance = await getPerformance('operator', operator.address, fromEpoch, toEpoch)
                hashedOperators[operator.address] = {
                    performance,
                    name: operator.name,
                    validatorsManaged: 0,
                    publicKey: operator.address,
                    ownerAddress: operator.owner_address,
                    verified: operator.type === 'verified_operator' || operator.type === 'dapp_node'
                }
            }
            console.log(`Processed operator: ${counter} / ${operators.length}`)
            ++counter
        });
        await Promise.all(processedOperators);
        await delay(2000);
    }
}


async function getValidators() {
    const {validators, pagination} = await getValidatorsRequest(1);
    const numOfPages = pagination.pages - 1;
    const allValidators = [...validators];
    for (let i in Array.from(Array(numOfPages).keys())) {
        const response = await getValidatorsRequest(Number(i) + 2)
        allValidators.push(...response.validators);
        console.log('fetched: ' + allValidators.length + '/' + response.pagination.total + ' validators');
    }
    return allValidators;
}

async function getOperators() {
    const {operators, pagination} = await getOperatorsRequest(1);
    const numOfPages = pagination.pages - 1;
    const allOperators = [...operators];
    for (let i in Array.from(Array(numOfPages).keys())) {
        const response = await getOperatorsRequest(Number(i) + 2)
        allOperators.push(...response.operators);
        console.log('fetched: ' + allOperators.length + '/' + response.pagination.total);
    }
    return allOperators;
}

async function getOperatorsRequest(page) {
    const response = await got.get(process.env.EXPLORER_URI + `/operators?per_page=2000&page=${page}`)
    return JSON.parse(response.body)
}

async function getValidatorsRequest(page) {
    const response = await got.get(process.env.EXPLORER_URI + `/validators?operators=true&per_page=2000&page=${page}`)
    return JSON.parse(response.body)
}

async function processValidators(fromEpoch, toEpoch, whiteList) {
    let counter = 1;
    const validators = await getValidators();
    const validatorsLength = validators.length;
    const batches = sliceIntoChunks(validators, Number(process.env.RATE_LIMIT));
    
    for (const batch of batches) {
        console.log('batch start')
        const processedValidators = batch.map(async (validator) => {
            const validatorPublicKey = validator.public_key.startsWith('0x') ? validator.public_key : `0x${validator.public_key}`;
            if (whiteList[validatorPublicKey]) {
                const performance = await getPerformance('validator', validator.public_key, fromEpoch, toEpoch)
                hashedValidators[validatorPublicKey] = {
                    performance,
                    publicKey: validatorPublicKey,
                    operators: validator.operators
                }
            }
            console.log('Processed validator: ' + counter + ' / ' + validatorsLength);
            ++counter
        })
        await Promise.all(processedValidators)
        console.log('batch end')
        await delay(2000)
    }
}

async function fetchOperatorsValidators(fromEpoch, toEpoch, whiteList) {
    await processOperators(fromEpoch, toEpoch)
    await processValidators(fromEpoch, toEpoch, whiteList)
    return {operators: hashedOperators, validators: hashedValidators};
}

async function getSsvBalances(ownersAddresses) {
    const hashedOwnersAddresses = {};
    const newOwnersAddresses = ownersAddresses.slice();
    return new Promise(async resolve => {
        const batches = new Array(Math.ceil(newOwnersAddresses.length / 10)).fill().map(_ => newOwnersAddresses.splice(0, 10))
        let counter = 1;
        for (const batch of batches) {
            const startTime = performance.now()
            await Promise.all(batch.map(cell => {
                console.log('get balance for: ' + counter + ' / ' + ownersAddresses.length + ` ${cell}`)
                ++counter
                return new Promise((resolveBalance) => {
                    getSsvBalance(cell).then((balance) => {
                        hashedOwnersAddresses[cell] = balance;
                        resolveBalance();
                    })
                });
            }));
            const endTime = performance.now()
            console.log('<<finish get balance batch>>');
            await sleep(5000 - (endTime - startTime))
        }
        resolve(hashedOwnersAddresses);
    });
}

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function getSsvBalance(ownerAddress) {
    return new Promise(resolve => {
        // (height: {lteq: 14313817})
        const query = `
        {
         ethereum(network: ethereum) {
          address(address: {is: "${ownerAddress}"}) {
           balances {
            currency {
             address
             symbol
             tokenType
            }
            value
           }
          }
         }
        }`;

        const url = "https://graphql.bitquery.io/";
        const opts = {
            headers: {
                "Content-Type": "application/json",
                "X-API-KEY": "BQYf1PUKSAly9e8dUtwLhO31sdjFmCe9"
            },
            body: JSON.stringify({
                query
            })
        };
        got.post(url, opts).then((res) => {
            const response = JSON.parse(res.body);
            let ownerAddressBalance = 0;
            if (response.data && response.data.ethereum !== null) {
                const balances = response.data.ethereum.address[0].balances
                if (balances) {
                    balances.forEach((balance) => {
                        if (balance.currency.address === '0x9d65ff81a3c488d585bbfb0bfe3c7707c7917f54') {
                            ownerAddressBalance = balance.value
                        }
                    })
                }
            }
            
            contract.methods.totalVestingBalanceOf(ownerAddress).call().then((amount) => {
                const vestedMoney = web3.utils.fromWei(amount);
                const allSsv = Number(ownerAddressBalance) + Number(vestedMoney);
                resolve(allSsv);
            })
        }).catch((e) => {
            console.log(`<<<<<<<<<calculate again>>>>>>>>> Error with ${ownerAddress}`)
            setTimeout(() => {
                resolve(getSsvBalance(ownerAddress));
            }, 1000)
        })
    })
}

async function getPerformance(type, publicKey, fromEpoch, toEpoch) {
    try {
        let balance = 0;
        const url = `${process.env.EXPLORER_URI}/${type + 's'}/incentivized?${type}=${publicKey}&network=prater&epochs=${fromEpoch}-${toEpoch}`;
        const response = await got.get(url)
        const itemPerformance = JSON.parse(response.body)
        if (itemPerformance.rounds.length > 0) {
            balance = itemPerformance.rounds[0].performance;
        }
        return balance;
    } catch (e) {
        console.log('<<<<<<<<<<<<<<<<<<<<<<<<<<<<error>>>>>>>>>>>>>>>>>>>>>>>>>>>>');
        console.log(e.message);
        console.log('sleep for: 3sec')
        await sleep(3000);
        return await getPerformance(type, publicKey, fromEpoch, toEpoch);
        console.log('<<<<<<<<<<<<<<<<<<<<<<<<<<<<error>>>>>>>>>>>>>>>>>>>>>>>>>>>>');
    }
}


module.exports = {
    uniq,
    getPerformance,
    getSsvBalances,
    fetchOperatorsValidators
}