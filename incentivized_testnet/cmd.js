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
const {uniq, fetchOperatorsValidators, getPerformance, getSsvBalance} = require('./helpers/utilis')

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
                aggr['operatorPublicKeys'] = row.returnValues[key]
                    .reduce((aggr, value) => {
                        aggr.push(convertPublickey(value.operatorPublicKey));
                        return aggr;
                    }, [])
                    .join(';');
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
    const hashedOperators = {};
    const hashedValidators = {};
    const contractValidators = {};
    const operatorByOwnerAddress = {};
    const validatorByOwnerAddress = {};

    const validatorsFile = `${__dirname}/validators.csv`;
    await stat(validatorsFile);

    const validatorsParser = fs.createReadStream(validatorsFile).pipe(parse({columns: true}));

    for await (const record of validatorsParser) {
        contractValidators[record.publicKey] = record.ownerAddress;
    }

    const operators = await fetchOperatorsValidators(process.env.EXPLORER_URI + '/operators/graph/');
    const validators = await fetchOperatorsValidators(process.env.EXPLORER_URI + '/validators/detailed?perPage=1000');

    console.log(`prepare operators`)
    for(const index in operators.slice(0,10)) {
        const operator = operators[index];
        console.log(`finish: ${Number.parseInt(index) + 1} / ${operators.length}`)
        const performance = await getPerformance('operator', operator.address, fromEpoch, toEpoch);
        const eligible = performance >= 1;
        const payload = {verified: operator.type === 'verified_operator', eligible, performance, validatorManaged: 0};
        hashedOperators[operator.address] = payload
        if (operatorByOwnerAddress[operator.owner_address.toLowerCase()]) {
            operatorByOwnerAddress[operator.owner_address.toLowerCase()].push(payload)
        } else {
            operatorByOwnerAddress[operator.owner_address.toLowerCase()] = [payload]
        }
    }

    console.log(`prepare validators`)
    for (const index in validators) {
        const validator = validators[index];
        const validatorPublicKey = validator.publicKey.startsWith('0x') ? validator.publicKey : `0x${validator.publicKey}`;
        const validatorOwnerAddress = contractValidators[validatorPublicKey];
        console.log(`finish: ${Number.parseInt(index) + 1} / ${validators.length}`)
        const performance = await getPerformance('validator', validatorPublicKey, fromEpoch, toEpoch);
        const payload = {publicKey: validatorPublicKey, performance, operators: [], eligible: performance >= 90};
        validator.operators.forEach(operator => {
            const operatorPublicKey = operator.address
            payload.operators.push(operatorPublicKey);
            if(hashedOperators[operatorPublicKey]) hashedOperators[operatorPublicKey].validatorManaged += 1
        })
        hashedValidators[validatorPublicKey] = payload
        if (validatorByOwnerAddress[validatorOwnerAddress]) {
            validatorByOwnerAddress[validatorOwnerAddress].push(payload)
        } else {
            validatorByOwnerAddress[validatorOwnerAddress] = [payload]
        }
    }
    const operatorCsv = [];

    // PREPARE CSV FOR OPERATORS (INDIVIDUAL)
    const operatorsPublicKeys = Object.keys(hashedOperators);
    for (const publicKey of operatorsPublicKeys){
        const operator = hashedOperators[publicKey]
        if(operator.eligible) {
            operatorCsv.unshift([publicKey, operator.performance, operator.eligible ? 'true' : 'false'])
        } else {
            operatorCsv.push([publicKey, operator.performance, operator.eligible ? 'true' : 'false'])
        }
    }
    operatorCsv.unshift(['PublicKey', 'Performance', 'Eligible (Yes / No)'])

    const validatorsCsv = [];

    // PREPARE CSV FOR VALIDATORS (INDIVIDUAL)
    const validatorsPublicKeys = Object.keys(hashedValidators);
    for (const publicKey of validatorsPublicKeys){
        const validator = hashedValidators[publicKey]
        if(validator.eligible) {
            validatorsCsv.unshift([publicKey, validator.performance, validator.eligible ? 'true' : 'false'])
        } else {
            validatorsCsv.push([publicKey, validator.performance, validator.eligible ? 'true' : 'false'])
        }
    }
    validatorsCsv.unshift(['PublicKey', 'Performance', 'Eligible (Yes / No)'])

    const ownerAddressCsv = [
        [
            'Owner Address',
            'verified operator avg. performance (%)',
            'verified operators (#)',
            'validators managed by verified operators (#)',
            'non-verified operators (#)',
            'non-verified operator avg. performance (%)',
            'validators managed by non-verified operators (#)',
            'validators (#)',
            'SSV Amount'
        ]
    ];
    const ownersAddresses = [...new Set([...Object.keys(validatorByOwnerAddress), ...Object.keys(operatorByOwnerAddress)])];
    for (const ownerAddress of ownersAddresses){
        let eligibleValidators = 0;
        let eligibleVerifiedOperators = 0;
        let verifiedOperatorsValidator = 0;
        let eligibleNonVerifiedOperators = 0;
        let nonVerifiedOperatorsValidator = 0;
        let verifiedOperatorsPerformanceAvg = 0
        let nonVerifiedOperatorsPerformanceAvg = 0
        let verifiedOperatorsCounter = 0;
        let nonVerifiedOperatorsCounter = 0;
        let ssvBalance = await getSsvBalance(ownerAddress);
        validatorByOwnerAddress[ownerAddress]?.forEach((validator)=>{
            if(validator.eligible) ++eligibleValidators
        })
        operatorByOwnerAddress[ownerAddress]?.forEach((operator)=>{
            if(operator.verified && operator.eligible) {
                ++verifiedOperatorsCounter;
                ++eligibleVerifiedOperators;
                verifiedOperatorsValidator += operator.validatorManaged;
                verifiedOperatorsPerformanceAvg += operator.performance;
            }
            if(!operator.verified && operator.eligible) {
                ++nonVerifiedOperatorsCounter
                ++eligibleNonVerifiedOperators;
                nonVerifiedOperatorsValidator += operator.validatorManaged;
                nonVerifiedOperatorsPerformanceAvg += operator.performance;
            }
            verifiedOperatorsPerformanceAvg = verifiedOperatorsPerformanceAvg / verifiedOperatorsCounter
            nonVerifiedOperatorsPerformanceAvg = nonVerifiedOperatorsPerformanceAvg / nonVerifiedOperatorsCounter

        })
        ownerAddressCsv.push(
            [
                ownerAddress,
                eligibleVerifiedOperators,
                verifiedOperatorsPerformanceAvg,
                verifiedOperatorsValidator,
                eligibleNonVerifiedOperators,
                nonVerifiedOperatorsPerformanceAvg,
                nonVerifiedOperatorsValidator,
                eligibleValidators,
                ssvBalance
            ]
        )
    }


    writeToFile(operatorCsv, `eligible_operators_${fromEpoch}-${toEpoch}`)
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

const {command, epochs} = commandLineArgs(argsDefinitions);

if (command === 'fetch') {
    fetchData();
} else if (command === 'metrics') {
    fetchValidatorMetrics(epochs[0], epochs[1]);
} else if( command === 'eligible') {
    createEligibleReport(epochs[0], epochs[1]);
}