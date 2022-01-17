const got = require('got');
const Web3 = require('web3');
function uniq(a) {
    return [...new Set(a)];
}

const web3 = new Web3(process.env.INFURA_URL);
const CONTRACT_ABI = require('../vestedContractAbi.json');
const contract = new web3.eth.Contract(CONTRACT_ABI, '0xB8471180C79A0a69C7790A1CCf62e91b3c3559Bf');

const hashedOperators = {};
const hashedValidators = {};

async function fetchOperators(fromEpoch, toEpoch) {
    return new Promise((resolve, reject) => {
        getOperators().then(async (operators) => {
            let counter = 1;
            const operatorsCount = operators.length;
            const batches = new Array(Math.ceil(operators.length / process.env.RATE_LIMIT)).fill().map(_ => operators.splice(0, process.env.RATE_LIMIT))
            for (const batch of batches) {
                for (const subBatch of batch) {
                    await new Promise((resolveOperators) => {
                        getPerformance('operator', subBatch.address, fromEpoch, toEpoch).then((performance) => {
                            console.log(`prepare Operator: ${counter} / ${operatorsCount}`)
                            ++counter
                            if (subBatch.owner_address !== '0x943a1b677da0ac80f380f08731fae841b1201402') {
                                hashedOperators[subBatch.address] = {
                                    performance,
                                    name: subBatch.name,
                                    validatorsManaged: 0,
                                    publicKey: subBatch.address,
                                    ownerAddress: subBatch.owner_address,
                                    verified: subBatch.type === 'verified_operator' || subBatch.type === 'dapp_node'
                                }
                            }
                            resolveOperators()
                        })
                    })
                }
            }
            resolve()
        }).catch((e) => {
            console.log('<<<<<<<<<<<error>>>>>>>>>>>');
            reject(e.message);
        })
    });
}

async function getValidators() {
    return new Promise(resolve => {
        got.get(process.env.EXPLORER_URI + '/validators/detailed?perPage=500&page=1').then(async (response) => {
            const data = JSON.parse(response.body)
            const numOfPages = data.pagination.pages - 1;
            const validators = [];
            for (let i in Array(numOfPages).fill(null)) {
                const loadValidators = await getValidatorsRequest(Number(i) + 2)
                validators.push(loadValidators);
            }
            resolve([...validators.flat(), ...data.validators]);
        }).catch(() => {
            resolve(getValidators());
        });
    })
}

async function getOperators() {
    return new Promise(async resolve => {
        got.get(process.env.EXPLORER_URI + '/operators/graph?perPage=200&page=1').then(async (response) => {
            const data = JSON.parse(response.body)
            const numOfPages = data.pagination.pages - 1;
            const operators = [];
            for (let i in Array(numOfPages).fill(null)) {
                const loadOperators = await getOperatorsRequest(Number(i) + 2)
                operators.push(loadOperators);
            }
            resolve([...operators.flat(), ...data.operators]);
        }).catch(() => {
            resolve(getOperators());
        });
    })
}

async function getOperatorsRequest(page) {
    return new Promise(resolve => {
        got.get(process.env.EXPLORER_URI + `/operators/graph?perPage=200&page=${page}`).then(async (response) => {
            const data = JSON.parse(response.body)
            resolve(data.operators);
        });
    })
}

async function getValidatorsRequest(page) {
    return new Promise(resolve => {
        got.get(process.env.EXPLORER_URI + `/validators/detailed?perPage=500&page=${page}`).then(async (response) => {
            const data = JSON.parse(response.body)
            resolve(data.validators);
        });
    })
}

async function fetchValidators(fromEpoch, toEpoch) {
    return new Promise((resolve, reject) => {
        getValidators().then(async (validators) => {
            let counter = 1;
            const validatorsLength = validators.length;
            const batches = new Array(Math.ceil(validators.length / process.env.RATE_LIMIT)).fill().map(_ => validators.splice(0, process.env.RATE_LIMIT))
            for (const batch of batches) {
                for (const subBatch of batch) {
                    await new Promise((resolveValidators) => {
                        const validatorPublicKey = subBatch.publicKey.startsWith('0x') ? subBatch.publicKey : `0x${subBatch.publicKey}`;
                        getPerformance('validator', subBatch.publicKey, fromEpoch, toEpoch).then((performance) => {
                            console.log('prepare Validator: ' + counter + ' / ' + validatorsLength);
                            ++counter
                            hashedValidators[validatorPublicKey] = {
                                publicKey: validatorPublicKey,
                                performance,
                                operators: subBatch.operators
                            }
                            resolveValidators()
                        })
                    })
                }
            }
            resolve();
        }).catch((e) => {
            console.log('<<<<<<<<<<<error>>>>>>>>>>>');
            reject(e.message);
        })
    })
}

async function fetchOperatorsValidators(fromEpoch, toEpoch) {
    return new Promise((resolve => {
        fetchOperators(fromEpoch, toEpoch).then(() => {
            fetchValidators(fromEpoch, toEpoch).then(() => {
                resolve({operators: hashedOperators, validators: hashedValidators});
            });
        })
    }))
}

async function getSsvBalances(ownersAddresses) {
    const hashedOwnersAddresses = {};
    const newOwnersAddresses = ownersAddresses.slice();
    return new Promise(async resolve => {
        const batches = new Array(Math.ceil(newOwnersAddresses.length / 1)).fill().map(_ => newOwnersAddresses.splice(0, 1))
        for (const batch of batches) {
            await Promise.all(batch.map(cell => {
                console.log(`get balance for: ${cell}`)
                return new Promise((resolveBalance) => {
                    getSsvBalance(cell).then((balance) => {
                        hashedOwnersAddresses[cell] = balance;
                        resolveBalance();
                    })
                });
            }));
        }
        resolve(hashedOwnersAddresses);
    });
}

async function getSsvBalance(ownerAddress) {
    return new Promise(resolve => {
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
                "X-API-KEY": "BQYlKR1yoTnzgkreLzL3QnQq2NmrPQOu"
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
                        if (balance.currency.symbol === 'SSV') {
                            ownerAddressBalance = balance.value
                        }
                    })
                }
            }

            contract.methods.totalVestingBalanceOf(ownerAddress).call().then((amount) => {
                const vestedMoney = web3.utils.fromWei(amount);
                resolve(ownerAddressBalance + vestedMoney);
            });
        }).catch((e) => {
            console.log(`Error with ${ownerAddress}`)
            console.log(`calculate again...`)
            setTimeout(() => {
                resolve(getSsvBalance(ownerAddress));
            }, 1000)
        })
    })
}

async function getPerformance(type, publicKey, fromEpoch, toEpoch) {
    return new Promise(resolve => {
        let balance = 0;
        console.log(`${process.env.EXPLORER_URI}/${type + 's'}/incentivized?${type}=${publicKey}&network=prater&epochs=${fromEpoch}-${toEpoch}`);
        got.get(`${process.env.EXPLORER_URI}/${type + 's'}/incentivized?${type}=${publicKey}&network=prater&epochs=${fromEpoch}-${toEpoch}`).then((response) => {
            const itemPerformance = JSON.parse(response.body)
            if (itemPerformance.rounds.length > 0) {
                balance = itemPerformance.rounds[0].performance;
            }
            resolve(balance);
        }).catch(() => {
            console.log('<<<<<<<<<<<<<<<<<<<<<<<<<<<<error>>>>>>>>>>>>>>>>>>>>>>>>>>>>');
            console.log(`${process.env.EXPLORER_URI}/${type + 's'}/incentivized?${type}=${publicKey}&network=prater&epochs=${fromEpoch}-${toEpoch}`);
            console.log('<<<<<<<<<<<<<<<<<<<<<<<<<<<<error>>>>>>>>>>>>>>>>>>>>>>>>>>>>');
            resolve(getPerformance(type, publicKey, fromEpoch, toEpoch))
        })
    })
}


module.exports = {
    uniq,
    getPerformance,
    getSsvBalances,
    fetchOperatorsValidators
}