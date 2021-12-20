// This is a mock request function, could be a `request` call
// or a database query; whatever it is, it MUST return a Promise.
const sendRequest = () => {
    return new Promise((resolve) => {
        setTimeout(() => {
            console.log('request sent')
            resolve()
        }, 1000)
    })
}

// 5 batches * 2 requests = 10 requests.
const batches = Array(5).fill(Array(2).fill(sendRequest))
console.log(batches);
;(async function() {
    for (const batch of batches) {
        try {
            console.log('-- sending batch --')
            await Promise.all(batch.map(f => f()))
        } catch(err) {
            console.error(err)
        }
    }
})()