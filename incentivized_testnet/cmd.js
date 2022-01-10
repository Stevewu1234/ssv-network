const fs = require('fs');
require('dotenv').config();
const got = require('got');
const Web3 = require('web3');
const crypto = require('crypto');
const parse = require('csv-parse');
const FormData = require('form-data');
const stringify = require('csv-stringify');
const {readFile, stat} = require('fs').promises;
const {Client} = require('@elastic/elasticsearch');
const commandLineArgs = require('command-line-args')
const {fetchOperatorsValidators, getSsvBalances} = require('./helpers/utilis')

const web3 = new Web3(process.env.NODE_URL);

const CONTRACT_ABI = require('./abi.json');
const contract = new web3.eth.Contract(CONTRACT_ABI, process.env.CONTRACT_ADDRESS);
const filteredFields = {
    // 'validators': ['oessList']
};
const client = new Client({
    node: process.env.ELASTICSEARCH_URI
});


function convertPublickey(rawValue) {
    const decoded = web3.eth.abi.decodeParameter('string', rawValue.replace('0x', ''));
    return crypto.createHash('sha256').update(decoded).digest('hex');
}

async function exportEventsData(dataType, fromBlock, latestBlock) {
    const eventName = dataType === 'operators' ? 'OperatorAdded' : 'ValidatorAdded';
    const filters = {
        fromBlock: fromBlock ? fromBlock + 1 : 0,
        toBlock: latestBlock
    };
    console.log(`fetching ${dataType}`, filters);
    const events = await contract.getPastEvents(eventName, filters);
    stringify(await getEventDetails(events, dataType), {
        header: !!!fromBlock
    }, (err, output) => {
        fs.appendFile(`${__dirname}/${dataType}.csv`, output, () => {
            console.log(`exported ${events.length} ${dataType}`)
        });
    });
};

async function extractOperatorsWithMetrics(operators, validatorsWithMetrics, operatorsDecided) {
    return operators.reduce((aggr, operator) => {
        const validators = validatorsWithMetrics.filter((validator) => {
            const operatorsPubkeys = validator.operatorPublicKeys.split(';');
            return !!operatorsPubkeys.find(okey => okey === operator.publicKey);
        });
        const attestationsAvg = (v) => (v.reduce((a, b) => a + +b.attestations, 0) / v.length).toFixed(0);
        operator.active = `${!!operatorsDecided.find(value => value.key === operator.name)}`;
        operator.validatorsCount = validators.length;
        operator.effectiveness = (validators.reduce((a, b) => a + +b.effectiveness, 0) / validators.length).toFixed(0);
        operator.attestations = attestationsAvg(validators);
        operator.attestationsWithout0 = attestationsAvg(validators.filter(v => {
            return v.active && v.attestations > 0
        }));
        aggr.push(operator);
        return aggr;
    }, []);
}

async function extractValidatorsWithMetrics(records, operators, operatorsDecided, fromEpoch, toEpoch) {
    const totalEpochs = toEpoch - fromEpoch;
    const MAX_EPOCHS_PER_REQUEST = +process.env.MAX_EPOCHS_PER_REQUEST || 100;
    let epochsPerRequest = 0;
    let lastEpoch = fromEpoch;
    while (lastEpoch + epochsPerRequest <= toEpoch) {
        if (epochsPerRequest === MAX_EPOCHS_PER_REQUEST || (lastEpoch + epochsPerRequest >= toEpoch)) {
            console.log(`fetching metrics for ${lastEpoch}-${lastEpoch + epochsPerRequest} epochs`, epochsPerRequest, fromEpoch, toEpoch);
            const form = new FormData();
            form.append('from', lastEpoch);
            form.append('to', lastEpoch + epochsPerRequest);
            form.append('keys', records.map(item => item.publicKey.replace('0x', '')).join(','));
            let response;
            try {
                const {body} = await got.post(`http://${process.env.BACKEND_URI}/api/validators/details`, {
                    body: form,
                    responseType: 'json'
                });
                response = body;
            } catch (e) {
                throw new Error(JSON.stringify(e.response.body));
            }

            records.forEach((item) => {
                item.active = `${!!response.find(value => value.PubKey === item.publicKey.replace('0x', ''))}`;
                item.shouldAttest = `${item.operatorPublicKeys.split(';').filter(itemOp => {
                    const opObj = operators.find(op => op.publicKey === itemOp);
                    return opObj !== undefined && !!operatorsDecided.find(decidedOp => decidedOp.key === opObj.name);
                }).length > 2}`;
                const eff = response.find(value => value.PubKey === item.publicKey.replace('0x', ''))?.Effectiveness || 0;
                const att = response.find(value => value.PubKey === item.publicKey.replace('0x', ''))?.Attestations?.Rate || 0;
                if (eff) {
                    item.effectiveness = item.effectiveness || [];
                    eff && item.effectiveness.push(eff);
                }
                if (att) {
                    item.attestations = item.attestations || [];
                    att && item.attestations.push(att);
                }
            });
            if (epochsPerRequest + 1 < toEpoch) {
                lastEpoch += epochsPerRequest + 1
            } else {
                lastEpoch = toEpoch;
            }
            epochsPerRequest = 0;
        } else {
            epochsPerRequest++;
        }
    }

    records.forEach(item => {
        if (Array.isArray(item.effectiveness)) {
            item.effectiveness = (item.effectiveness.reduce((a, b) => a + b, 0) / item.effectiveness.length * 100).toFixed(0);
        } else {
            item.effectiveness = 0;
        }

        if (Array.isArray(item.attestations)) {
            item.attestations = (item.attestations.reduce((a, b) => a + b, 0) / item.attestations.length * 100).toFixed(0);
        } else {
            item.attestations = 0;
        }
    });

    return records;
}

async function getEventDetails(events, dataType) {
    return events.map(row => Object.keys(row.returnValues)
        .filter(key => isNaN(key))
        .filter(key => !(filteredFields[dataType] && filteredFields[dataType].indexOf(key) !== -1))
        .reduce((aggr, key) => {
            if (dataType === 'operators' && key === 'publicKey') {
                aggr[key] = convertPublickey(row.returnValues[key]);
            } else if (dataType === 'validators' && key === 'oessList') {
                aggr['operatorPublicKeys'] = row.returnValues[key].map((value) => {
                    return value.operatorPublicKey;
                }).join(';');
            } else {
                aggr[key] = row.returnValues[key];
            }
            return aggr;
        }, {})
    );
};

async function fetchData() {
    const cacheFile = `${__dirname}/.process.cache`;
    fs.stat(cacheFile, async (err, stat) => {
        let blockFromCache;
        if (err == null) {
            blockFromCache = +(await readFile(cacheFile, 'utf8'));
        }
        const latestBlock = await web3.eth.getBlockNumber();
        await exportEventsData('operators', blockFromCache, latestBlock);
        await exportEventsData('validators', blockFromCache, latestBlock);
        fs.writeFile(cacheFile, `${latestBlock}`, () => {
        });
    });
}

async function createEligibleReport(fromEpoch, toEpoch) {
    const ssvHoldersAlloc = 6400;
    const allOperatorAlloc = 2240;
    const allValidatorAlloc = 1920;
    const verifiedOperatorAlloc = 2240;
    const validatorCoefficient = 50;
    const contractValidators = {};
    const operatorByOwnerAddress = {};
    const validatorByOwnerAddress = {};

    const validatorsFile = `${__dirname}/validators.csv`;
    await stat(validatorsFile);

    const validatorsParser = fs.createReadStream(validatorsFile).pipe(parse({columns: true}));

    for await (const record of validatorsParser) {
        contractValidators[record.publicKey] = record.ownerAddress;
    }

    const {operators, validators} = await fetchOperatorsValidators(fromEpoch, toEpoch);

    console.log(`Division of validators to ownerAddress`)
    for (const publicKey of Object.keys(validators)) {
        const validator = validators[publicKey];
        let validatorOwnerAddress = contractValidators[publicKey];
        if (validatorOwnerAddress) validatorOwnerAddress = validatorOwnerAddress.toLowerCase();
        if (validator.performance >= process.env.MINIMUM_ELIGIBLE_SCORE) {
            validator.operators.forEach(operator => {
                if (operators[operator.address]) operators[operator.address].validatorsManaged += 1
            })
            if (validatorByOwnerAddress[validatorOwnerAddress]) {
                validatorByOwnerAddress[validatorOwnerAddress].push(validator)
            } else {
                validatorByOwnerAddress[validatorOwnerAddress] = [validator]
            }
        }
    }

    console.log(`Division of operators to ownerAddress`)
    for (const publicKey of Object.keys(operators)) {
        const operator = operators[publicKey];
        if (operator.performance >= process.env.MINIMUM_ELIGIBLE_SCORE) {
            if (operatorByOwnerAddress[operator.ownerAddress.toLowerCase()]) {
                operatorByOwnerAddress[operator.ownerAddress.toLowerCase()].push(operator)
            } else {
                operatorByOwnerAddress[operator.ownerAddress.toLowerCase()] = [operator]
            }
        }
    }

    // PREPARE CSV FOR OWNER ADDRESS INCENTIVES
    const ownersAddresses = [...new Set([...Object.keys(validatorByOwnerAddress), ...Object.keys(operatorByOwnerAddress)])]
    const ownerAddressBalance = await getSsvBalances(ownersAddresses)

    const utilities = {
        allOperatorsWeight: 0,
        allValidatorsWeight: 0,
        verifiedOperatorsWeight: 0,
        ssvHoldersValidatorsWeight: 0,
    };

    for (const ownerAddress of ownersAddresses) {
        let verifiedOperatorsCount = 0;
        let operatorsAvgPerformance = 0;
        let allOperatorsCount = 0;
        let operatorsManagedValidators = 0;
        let verifiedOperatorsAvgPerformance = 0;
        let verifiedOperatorsManagedValidators = 0;
        let ssvBalance = ownerAddressBalance[ownerAddress];
        const validators = validatorByOwnerAddress[ownerAddress]?.length ?? 0;
        const weight = ssvBalance * Math.log(validatorCoefficient * validators + 1);

        operatorByOwnerAddress[ownerAddress]?.forEach((operator) => {
            if (operator.verified) {
                ++verifiedOperatorsCount
                verifiedOperatorsAvgPerformance += operator.performance;
                verifiedOperatorsManagedValidators += operator.validatorsManaged;
            }
            ++allOperatorsCount
            operatorsAvgPerformance += operator.performance;
            operatorsManagedValidators += operator.validatorsManaged;
        });
        if (operatorsAvgPerformance > 0) {
            operatorsAvgPerformance = operatorsAvgPerformance / allOperatorsCount
        }
        if (verifiedOperatorsAvgPerformance > 0) {
            verifiedOperatorsAvgPerformance = verifiedOperatorsAvgPerformance / verifiedOperatorsCount
        }
        utilities.allValidatorsWeight += validators;
        utilities.ssvHoldersValidatorsWeight += weight;
        utilities.allOperatorsWeight += operatorsAvgPerformance * operatorsManagedValidators;
        utilities.verifiedOperatorsWeight += verifiedOperatorsAvgPerformance * verifiedOperatorsManagedValidators;

    }

    // PREPARE CSV FOR VALIDATORS (INDIVIDUAL)
    const validatorsCsv = [['owner address', 'ssv', 'validators', 'weight', 'reward ssv', 'reward non ssv', 'total']];
    for (const ownerAddress of [...Object.keys(validatorByOwnerAddress)]) {
        const balance = ownerAddressBalance[ownerAddress];
        const validators = validatorByOwnerAddress[ownerAddress].length;
        const weight = balance * Math.log(validatorCoefficient * validators + 1);
        const rewardSsv = weight / utilities.ssvHoldersValidatorsWeight * ssvHoldersAlloc;
        const rewardNonSsv = validators / utilities.allValidatorsWeight * allValidatorAlloc;
        const total = rewardSsv + rewardNonSsv;
        validatorsCsv.push([
            ownerAddress,
            balance,
            validators,
            weight,
            rewardSsv,
            rewardNonSsv,
            total
        ])
    }
    ///////////////////////////////////////////

    // PREPARE CSV FOR OPERATORS (INDIVIDUAL)
    const operatorsCsv = [[
        'owner address',
        'verified operators',
        'verified performance',
        'verified validators',
        'verified weight',
        'verified reward',
        'all operators',
        'all performance',
        'all validators',
        'all weight',
        'all reward',
        'total'
    ]];
    for (const ownerAddress of [...Object.keys(operatorByOwnerAddress)]) {
        let verifiedOperators = 0;
        let verifiedOperatorsReward = 0;
        let verifiedOperatorsValidators = 0;
        let verifiedOperatorsPerformance = 0;

        let allOperators = 0;
        let allOperatorsReward = 0;
        let allOperatorsValidators = 0;
        let allOperatorsPerformance = 0;

        operatorByOwnerAddress[ownerAddress].map((operator) => {
            if (operator.verified) {
                ++verifiedOperators;
                verifiedOperatorsPerformance += operator.performance;
                verifiedOperatorsValidators += operator.validatorsManaged;
            }
            ++allOperators;
            allOperatorsPerformance += operator.performance;
            allOperatorsValidators += operator.validatorsManaged;
        })
        if (verifiedOperatorsPerformance > 0) verifiedOperatorsPerformance = verifiedOperatorsPerformance / verifiedOperators;
        if (allOperatorsPerformance > 0) allOperatorsPerformance = allOperatorsPerformance / allOperators;
        let verifiedWeight = verifiedOperatorsPerformance * verifiedOperatorsValidators;
        let allOperatorsWeight = allOperatorsPerformance * allOperatorsValidators;
        if (verifiedWeight > 0) verifiedOperatorsReward = verifiedWeight / utilities.verifiedOperatorsWeight * verifiedOperatorAlloc;
        if (allOperatorsWeight > 0) allOperatorsReward = allOperatorsWeight / utilities.allOperatorsWeight * allOperatorAlloc;
        let total = verifiedOperatorsReward + allOperatorsReward
        operatorsCsv.push([
            ownerAddress,
            verifiedOperators,
            verifiedOperatorsPerformance,
            verifiedOperatorsValidators,
            verifiedWeight,
            verifiedOperatorsReward,
            allOperators,
            allOperatorsPerformance,
            allOperatorsValidators,
            allOperatorsWeight,
            allOperatorsReward,
            total
        ])
    }
    ///////////////////////////////////////////

    const ownerAddressCsv = [
        [
            'Owner Address',
            'verified operators (#)',
            'verified operator avg. performance (%)',
            'validators managed by verified operators (#)',
            'non-verified operators (#)',
            'non-verified operator avg. performance (%)',
            'validators managed by non-verified operators (#)',
            'validators (#)',
            'SSV Amount',
            'Reward Validators',
            'reward validators with ssv',
            'reward Verified operator',
            'reward All operator',
            'total rewards'
        ]
    ];

    for (const ownerAddress of ownersAddresses) {
        let verifiedOperatorsCounter = 0;
        let verifiedOperatorsPerformance = 0;
        let validatorsManagedByVerifiedOperators = 0;

        let allOperators = 0;
        let allOperatorsPerformance = 0;
        let allOperatorsValidators = 0;

        let ownerAddressValidators = 0;
        let ssvBalance = ownerAddressBalance[ownerAddress];

        let rewardAllValidators = 0;
        let rewardValidatorsWithSsv = 0;
        let rewardVerifiedOperators = 0;
        let rewardAllOperators = 0;
        let totalReward = 0;

        validatorByOwnerAddress[ownerAddress]?.forEach((validator) => {
            ++ownerAddressValidators;
        })

        operatorByOwnerAddress[ownerAddress]?.forEach((operator) => {
            if (operator.verified) {
                ++verifiedOperatorsCounter;
                verifiedOperatorsPerformance += operator.performance;
                validatorsManagedByVerifiedOperators += operator.validatorsManaged;
            }
            ++allOperators
            allOperatorsPerformance += operator.performance;
            allOperatorsValidators += operator.validatorsManaged;
        })

        if (verifiedOperatorsPerformance > 0) verifiedOperatorsPerformance = verifiedOperatorsPerformance / verifiedOperatorsCounter;
        if (allOperatorsPerformance > 0) allOperatorsPerformance = allOperatorsPerformance / allOperators
        if (ownerAddressValidators > 0) rewardAllValidators = ownerAddressValidators / utilities.allValidatorsWeight * allValidatorAlloc;
        if (ssvBalance > 0 && ownerAddressValidators > 0) {
            rewardValidatorsWithSsv = ssvBalance * Math.log(validatorCoefficient * ownerAddressValidators + 1) / utilities.ssvHoldersValidatorsWeight * ssvHoldersAlloc
        }
        if (verifiedOperatorsCounter > 0 && verifiedOperatorsPerformance > 0) {
            rewardVerifiedOperators = verifiedOperatorsPerformance * validatorsManagedByVerifiedOperators / utilities.verifiedOperatorsWeight * verifiedOperatorAlloc;
        }
        if (allOperatorsPerformance > 0) {
            rewardAllOperators = allOperatorsPerformance * allOperatorsValidators / utilities.allOperatorsWeight * allOperatorAlloc;
        }
        totalReward = rewardAllValidators + rewardValidatorsWithSsv + rewardVerifiedOperators + rewardAllOperators;

        if (totalReward > 0) {
            ownerAddressCsv.push(
                [
                    ownerAddress,
                    verifiedOperatorsCounter,
                    verifiedOperatorsPerformance,
                    validatorsManagedByVerifiedOperators,
                    allOperators,
                    allOperatorsPerformance,
                    allOperatorsValidators,
                    ownerAddressValidators,
                    ssvBalance,
                    rewardAllValidators,
                    rewardValidatorsWithSsv,
                    rewardVerifiedOperators,
                    rewardAllOperators,
                    totalReward
                ]
            )
        }
    }

    const headers = [['Owner Address', 'validators (#)', 'operators (#)']]
    for (const ownerAddress of ownersAddresses) {
        let validators = 0;
        let operators = 0;
        validatorByOwnerAddress[ownerAddress]?.forEach((validator) => {
            ++validators;
        })

        operatorByOwnerAddress[ownerAddress]?.forEach((operator) => {
            ++operators
        })
        headers.push([ownerAddress, validators, operators]);
    }

    writeToFile(headers, `ownerAddress_distribution_${fromEpoch}-${toEpoch}`)
    writeToFile(operatorsCsv, `eligible_operators_${fromEpoch}-${toEpoch}`)
    writeToFile(validatorsCsv, `eligible_validators_${fromEpoch}-${toEpoch}`)
    writeToFile(ownerAddressCsv, `eligible_ownerAddress_${fromEpoch}-${toEpoch}`)
    console.log('done')
}

function writeToFile(data, fileName) {
    stringify(data, (err, output) => {
        console.log('error: ' + err)
        console.log(output)
        fs.writeFile(`${__dirname}/${fileName}.csv`, output, () => {
            console.log(`file: ${__dirname}/${fileName}.csv exported`)
        });
    });
}

async function fetchValidatorMetrics(fromEpoch, toEpoch) {
    const operatorsFile = `${__dirname}/operators.csv`;
    const validatorsFile = `${__dirname}/validators.csv`;
    await stat(validatorsFile);
    await stat(operatorsFile);

    const validators = [];
    const valdatorParser = fs
        .createReadStream(validatorsFile)
        .pipe(parse({
            columns: true
        }));

    for await (const record of valdatorParser) {
        validators.push(record);
    }

    const operators = [];
    const operatorParser = fs
        .createReadStream(operatorsFile)
        .pipe(parse({
            columns: true
        }));

    for await (const record of operatorParser) {
        operators.push(record);
    }

    const operatorsDecided = await extractOperatorsDecided(fromEpoch, toEpoch);
    const validatorsWithMetrics = await extractValidatorsWithMetrics(validators, operators, operatorsDecided, fromEpoch, toEpoch);
    const operatorsWithMetrics = await extractOperatorsWithMetrics(operators, validatorsWithMetrics, operatorsDecided);

    stringify(validatorsWithMetrics, {
        header: true
    }, (err, output) => {
        fs.writeFile(`${__dirname}/validators_extra_${fromEpoch}-${toEpoch}.csv`, output, () => {
            console.log(`exported ${validatorsWithMetrics.length} validator metrics records`)
        });
    });

    stringify(operatorsWithMetrics, {
        header: true
    }, (err, output) => {
        fs.writeFile(`${__dirname}/operators_extra_${fromEpoch}-${toEpoch}.csv`, output, () => {
            console.log(`exported ${operatorsWithMetrics.length} operators metrics records`)
        });
    });
}

function extractOperatorsDecided(fromEpoch, toEpoch) {
    const res = client.search({
        index: 'decided_search',
        body: {
            query: {
                range: {
                    "message.value.Attestation.data.source.epoch": {
                        "gte": fromEpoch,
                        "lte": toEpoch
                    }
                }
            },
            aggs: {
                op: {
                    terms: {
                        field: "signer_ids.address.keyword",
                        size: 10000
                    }
                }
            },
            size: 0
        }
    });
    return res.catch(err => {
        throw new Error(JSON.stringify(err));
    }).then(res => {
        return res.body.aggregations.op.buckets
    })
}

const argsDefinitions = [
    {name: 'command', type: String},
    {name: 'epochs', type: Number, multiple: true},
];

const {command, epochs, env} = commandLineArgs(argsDefinitions);

if (command === 'fetch') {
    fetchData();
} else if (command === 'metrics') {
    fetchValidatorMetrics(epochs[0], epochs[1]);
} else if (command === 'eligible') {
    createEligibleReport(epochs[0], epochs[1]);
}