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
const delay = timeToWait => new Promise(resolve => setTimeout(resolve, timeToWait));

async function fetchOperators(fromEpoch, toEpoch) {
    return new Promise((resolve) => {
        getOperators().then(async (operators) => {
            let counter = 1;
            const operatorsCount = operators.length;
            const batches = new Array(Math.ceil(operators.length / process.env.RATE_LIMIT)).fill().map(_ => operators.splice(0, process.env.RATE_LIMIT))
            for (const batch of batches) {
                const operators = batch.map((operator) => {
                    return new Promise(async (resolveOperator) => {
                        if (operator.owner_address === '0x943a1b677da0ac80f380f08731fae841b1201402'){
                            resolveOperator();
                            return
                        }
                        
                        const performance = await getPerformance('operator', operator.address, fromEpoch, toEpoch)
                        console.log(performance);
                        console.log(`prepare Operator: ${counter} / ${operatorsCount}`)
                        ++counter
                        hashedOperators[operator.address] = {
                            performance,
                            name: operator.name,
                            validatorsManaged: 0,
                            publicKey: operator.address,
                            ownerAddress: operator.owner_address,
                            verified: operator.type === 'verified_operator' || operator.type === 'dapp_node'
                        }
                        resolveOperator()
                    });
                })
                await Promise.all(operators)
                await delay(2000)
            }
            resolve()
        }).catch((e) => {
            console.log(e);
            console.log('<<<<<<<<<<<error1>>>>>>>>>>>');
            // reject(e.message);
        })
    })
}


async function getValidators() {
    return new Promise(resolve => {
        got.get(process.env.EXPLORER_URI + '/validators?operators=true&per_page=2000&page=1').then(async (response) => {
            const data = JSON.parse(response.body)
            const numOfPages = data.pagination.pages - 1;
            const validators = [];
            for (let i in Array(numOfPages).fill(null)) {
                console.log(i);
                const loadValidators = await getValidatorsRequest(Number(i) + 2)
                validators.push(loadValidators);
            }
            console.log('finsihed')
            resolve([...validators.flat(), ...data.validators]);
        }).catch(() => {
            resolve(getValidators());
        });
    })
}

async function getOperators() {
    return new Promise(async resolve => {
        got.get(process.env.EXPLORER_URI + '/operators?per_page=2000&page=1').then(async (response) => {
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
        got.get(process.env.EXPLORER_URI + `/operators/graph?per_page=2000&page=${page}`).then(async (response) => {
            const data = JSON.parse(response.body)
            resolve(data.operators);
        });
    })
}

async function getValidatorsRequest(page) {
    return new Promise(resolve => {
        got.get(process.env.EXPLORER_URI + `/validators?operators=true&per_page=2000&page=${page}`).then(async (response) => {
            const data = JSON.parse(response.body)
            resolve(data.validators);
        });
    })
}

async function fetchValidators(fromEpoch, toEpoch, whiteList) {
    return new Promise((resolve, reject) => {
        getValidators().then(async (validators) => {
            let counter = 1;
            const validatorsLength = validators.length;
            const batches = new Array(Math.ceil(validators.length / process.env.RATE_LIMIT)).fill().map(_ => validators.splice(0, process.env.RATE_LIMIT))
            for (const batch of batches) {
                const validators = batch.map((validator) => {
                    return new Promise(async (resolveValidator) => {
                        const validatorPublicKey = validator.public_key.startsWith('0x') ? validator.public_key : `0x${validator.public_key}`;
                        if (!whiteList[validatorPublicKey]) {
                            ++counter
                            resolveValidator();
                            return;
                        }
                        const performance = await getPerformance('validator', validator.public_key, fromEpoch, toEpoch)
                        console.log('prepare Validator: ' + counter + ' / ' + validatorsLength);
                        ++counter
                        hashedValidators[validatorPublicKey] = {
                            performance,
                            publicKey: validatorPublicKey,
                            operators: validator.operators
                        }
                        resolveValidator()
                    })
                })
                
                await Promise.all(validators)
                await delay(2000)
            }
            resolve();
        }).catch((e) => {
            console.log('<<<<<<<<<<<error on validators>>>>>>>>>>>');
            console.log(e.message);
            console.log('<<<<<<<<<<<error on validators>>>>>>>>>>>');
            reject(e.message);
        })
    })
}

async function fetchOperatorsValidators(fromEpoch, toEpoch, whiteList) {
    return new Promise((resolve => {
        fetchOperators(fromEpoch, toEpoch).then(() => {
            fetchValidators(fromEpoch, toEpoch, whiteList).then(() => {
                resolve({operators: hashedOperators, validators: hashedValidators});
            });
        })
    }))
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
        const query = `
        {
         ethereum(network: ethereum) {
          address(address: {is: "${ownerAddress}"}) {
           balances(height: {lteq: 14313817}) {
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
    return new Promise(resolve => {
        let balance = 0;
        console.log(`${process.env.EXPLORER_URI}/${type + 's'}/incentivized?${type}=${publicKey}&network=prater&epochs=${fromEpoch}-${toEpoch}`);
        got.get(`${process.env.EXPLORER_URI}/${type + 's'}/incentivized?${type}=${publicKey}&network=prater&epochs=${fromEpoch}-${toEpoch}`).then((response) => {
            const itemPerformance = JSON.parse(response.body)
            if (itemPerformance.rounds.length > 0) {
                balance = itemPerformance.rounds[0].performance;
            }
            resolve(balance);
        }).catch((e) => {
            console.log('<<<<<<<<<<<<<<<<<<<<<<<<<<<<error>>>>>>>>>>>>>>>>>>>>>>>>>>>>');
            console.log(e);
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