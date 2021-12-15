const got = require('got');
function uniq(a) {
    return [... new Set(a)];
}

async function fetchOperatorsValidators(url) {
    return new Promise(resolve => {
        got.get(url).then((response) => {
            const data = JSON.parse(response.body)
            resolve(data.validators ?? data.operators);
        }).catch(() => {
            resolve(0);
        })
    })
}

function getSsvBalance(ownerAddress) {
    return new Promise(resolve => {
        const query = `
        {
         ethereum(network: goerli) {
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
                balances.forEach((balance) => {
                    if (balance.currency.symbol === 'SSV') {
                        resolve(balance.value);
                    }
                })
                resolve(0);
            }
        })
    })
}

async function getPerformance(type, publicKey, fromEpoch, toEpoch) {
    return new Promise(resolve => {
        // console.log(`${process.env.EXPLORER_URI}/${type + 's'}/incentivized/?${type}=${publicKey}&network=prater&epochs=${fromEpoch}-${toEpoch}`)
        got.get(`${process.env.EXPLORER_URI}/${type + 's'}/incentivized/?${type}=${publicKey}&network=prater&epochs=${fromEpoch}-${toEpoch}`).then((response) => {
            const performance = JSON.parse(response.body)
            if (performance.rounds.length === 0) {
                setTimeout(() => resolve(0), 1000);
            }
            resolve(performance.rounds[0].performance);
        }).catch(() => {
            resolve(0);
        })
    })
}

module.exports =  {
    uniq,
    fetchOperatorsValidators,
    getPerformance,
    getSsvBalance
}