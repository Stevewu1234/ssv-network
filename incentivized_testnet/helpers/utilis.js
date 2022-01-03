const got = require('got');

function uniq(a) {
    return [...new Set(a)];
}

const hashedOperators = {};
const hashedValidators = {};

async function fetchOperators(fromEpoch, toEpoch) {
    return new Promise(resolve => {
        got.get(process.env.EXPLORER_URI + '/operators/graph/').then(async (response) => {
            let counter = 1;
            const data = JSON.parse(response.body)
            const operators = data.operators;
            const operatorsCount = operators.length;
            const batches = new Array(Math.ceil(operators.length / process.env.RATE_LIMIT)).fill().map(_ => operators.splice(0, process.env.RATE_LIMIT))
            for (const batch of batches) {
                const performances = await Promise.all(batch.map(cell => {
                    return new Promise((resolveOperators) => {
                        getPerformance('operator', cell.address, fromEpoch, toEpoch).then((performance) => {
                            console.log(`prepare Operator: ${counter} / ${operatorsCount}`)
                            ++counter
                            resolveOperators(
                                {
                                    performance,
                                    name: cell.name,
                                    validatorsManaged: 0,
                                    publicKey: cell.address,
                                    ownerAddress: cell.owner_address,
                                    verified: cell.type === 'verified_operator' || cell.type === 'dapp_node'
                                }
                            )
                        })
                    })
                }))
                performances.forEach(operator => hashedOperators[operator.publicKey] = operator)
            }
            resolve()
        }).catch(()=>{
            resolve(fetchOperators(fromEpoch, toEpoch));
        })
    });
}

async function getValidators() {
    return new Promise(resolve => {
        got.get(process.env.EXPLORER_URI + '/validators/detailed?perPage=500&page=1').then(async (response) => {
            const data = JSON.parse(response.body)
            const numOfPages = data.pagination.pages;
            const validators = await Promise.all(Array(numOfPages).fill(null).map((_, i) => getValidatorsRequest(i + 2)));
            resolve([...validators.flat(),...data.validators]);
        }).catch(()=>{
            resolve(getValidators());
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
    return new Promise(resolve => {
        getValidators().then(async (validators) => {
            // validators = validators.slice(0,100);
            let counter = 1;
            const validatorsLength = validators.length;
            const batches = new Array(Math.ceil(validators.length / process.env.RATE_LIMIT)).fill().map(_ => validators.splice(0, process.env.RATE_LIMIT))
            for (const batch of batches) {
                const performances = await Promise.all(batch.map(cell => {
                    return new Promise((resolveValidators) => {
                        const validatorPublicKey = cell.publicKey.startsWith('0x') ? cell.publicKey : `0x${cell.publicKey}`;
                        getPerformance('validator', cell.publicKey, fromEpoch, toEpoch).then((performance) => {
                            console.log('prepare Validator: ' + counter + ' / ' + validatorsLength)
                            ++counter
                            resolveValidators({
                                publicKey: validatorPublicKey,
                                performance,
                                operators: cell.operators
                            })
                        })
                    })
                }))
                performances.forEach(validator => hashedValidators[validator.publicKey] = validator)
            }
            resolve();
        })
    })
}

async function fetchOperatorsValidators(fromEpoch, toEpoch) {
    return new Promise((resolve => {
        fetchOperators(fromEpoch, toEpoch).then(() => {
            fetchValidators(fromEpoch, toEpoch).then(() => {
                delete hashedValidators['0x943a1b677da0ac80f380f08731fae841b1201402']
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
            if (response.data && response.data.ethereum !== null) {
                const balances = response.data.ethereum.address[0].balances
                if (balances) {
                    balances.forEach((balance) => {
                        if (balance.currency.symbol === 'SSV') {
                            resolve(balance.value)
                        }
                    })
                }
                resolve(0)
            } else {
                resolve(0)
            }
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
        got.get(`${process.env.EXPLORER_URI}/${type + 's'}/incentivized/?${type}=${publicKey}&network=prater&epochs=${fromEpoch}-${toEpoch}`).then((response) => {
            const performance = JSON.parse(response.body)
            if (performance.rounds.length === 0) {
                resolve(0);
            }
            resolve(performance.rounds[0].performance);
        }).catch(() => {
            resolve(0);
        })
    })
}


module.exports = {
    uniq,
    getValidators,
    getPerformance,
    getSsvBalances,
    fetchOperatorsValidators
}