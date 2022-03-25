require('dotenv').config()


const Redis = require("ioredis");

const sleep = ms => {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
};

const jsonGet = async (redis_instance, collection, expression) => {
    try {
        return await redis_instance.call('JSON.GET', collection, expression);
    } catch (err) {
       if  (String(err.message).includes('does not exist at level')) {
           return null;
        } else {
            throw err;
        }
        
    }
};


const asyncForEach = async (array, callback) => {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
};


const populate = async () => {

    const str = process.env.NEW_REDIS_CLUSTER
    const cluster = JSON.parse(str)
    const nat  = process.env.NEW_REDIS_CLUSTER_NAT
    const password = process.env.NEW_REDIS_PASSWORD

    const options = {
        redisOptions: {
            password,
        },
        }

    if (nat) {
        options['natMap']= JSON.parse(nat)
    }

    const start = Date.now();

    const redis_new  = new Redis.Cluster(
        cluster, options
        
    );

    const redis_old = new Redis(Number(process.env.OLD_REDIS_PORT), process.env.OLD_REDIS_HOST, { password: process.env.OLD_REDIS_PASSWORD });

    const blockchain = await redis_old.hgetall('blockchain')


    let keys = Object.keys(blockchain)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('blockchain', key, blockchain[key])
        const data = await redis_new.hget('blockchain', key)
    });
        

    const dailyATR = await redis_old.hgetall('dailyATR')

    keys = Object.keys(dailyATR)


    await asyncForEach(keys, async (key) => {
        await redis_new.hset('dailyATR', key, dailyATR[key])
    });

    const dat = await redis_new.hgetall('dailyATR')

    const mainchain = await redis_old.hgetall('mainchain')

    keys = Object.keys(mainchain)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('mainchain', key, mainchain[key])
    });

    const marketDownEmailSent = await redis_old.hgetall('marketDownEmailSent')

    keys = Object.keys(marketDownEmailSent)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('marketDownEmailSent', key, marketDownEmailSent[key])
    });

    const marketMovement = await redis_old.hgetall('marketMovement')

    keys = Object.keys(marketMovement)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('marketMovement', key, marketMovement[key])
    });

    const merkleProof = await redis_old.hgetall('merkleProof')

    keys = Object.keys(merkleProof)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('merkleProof', key, merkleProof[key])
    });

    const providersLastTicks = await redis_old.hgetall('providersLastTicks')

    keys = Object.keys(providersLastTicks)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('providersLastTicks', key, providersLastTicks[key])
    });

    const sidechain = await redis_old.hgetall('sidechain')

    keys = Object.keys(sidechain)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('sidechain', key, sidechain[key])
    });

    const sources = await redis_old.hgetall('sources')

    keys = Object.keys(sources)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('sources', key, sources[key])
    });

    const statistics = await redis_old.hgetall('statistics')

    keys = Object.keys(statistics)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('statistics', key, statistics[key])
    });

    const statistics_mainchain = await redis_old.hgetall('statistics_mainchain')

    keys = Object.keys(statistics_mainchain)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('statistics_mainchain', key, statistics_mainchain[key])
    });

    const stockSpreadATR = await redis_old.hgetall('stockSpreadATR')

    keys = Object.keys(stockSpreadATR)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('stockSpreadATR', key, stockSpreadATR[key])
    });

    const symbolsLastTicks = await redis_old.hgetall('symbolsLastTicks')

    keys = Object.keys(symbolsLastTicks)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('symbolsLastTicks', key, symbolsLastTicks[key])
    });

    const testchain = await redis_old.hgetall('testchain')

    keys = Object.keys(testchain)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('testchain', key, testchain[key])
    });

    const workers = await redis_old.hgetall('workers')

    keys = Object.keys(workers)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('workers', key, workers[key])
    });

    const config = JSON.parse(await jsonGet(redis_old, 'config', '.'))

    keys = Object.keys(config)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('config', key, config[key])
    });    

    const exchanges = JSON.parse(await jsonGet(redis_old, 'exchanges', '.'))

    keys = Object.keys(exchanges)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('exchanges', key, JSON.stringify(exchanges[key]))
    });    

    const markets = JSON.parse(await jsonGet(redis_old, 'markets', '.'))

    keys = Object.keys(markets)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('markets', key, JSON.stringify(markets[key]))
    });    

    const marketSegment = JSON.parse(await jsonGet(redis_old, 'marketSegment', '.'))

    keys = Object.keys(marketSegment)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('marketSegment', key, JSON.stringify(marketSegment[key]))
    });    

    const minutelyData = JSON.parse(await jsonGet(redis_old, 'minutelyData', '.'))

    keys = Object.keys(minutelyData)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('minutelyData', key, JSON.stringify(minutelyData[key]))
    });    

    const other = JSON.parse(await jsonGet(redis_old, 'other', '.'))

    keys = Object.keys(other)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('other', key, JSON.stringify(other[key]))
    });    

    const refreshTokens = JSON.parse(await jsonGet(redis_old, 'refreshTokens', '.'))

    keys = Object.keys(refreshTokens)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('refreshTokens', key, JSON.stringify(refreshTokens[key]))
    });    

    const stockSplits = JSON.parse(await jsonGet(redis_old, 'stockSplits', '.'))

    keys = Object.keys(stockSplits)

    await asyncForEach(keys, async (key) => {
        await redis_new.hset('stockSplits', key, JSON.stringify(stockSplits[key]))
    });    

    const livePositions = JSON.parse(await jsonGet(redis_old, 'livePositions', '.'))
    
    keys = Object.keys(livePositions)

    await asyncForEach(keys, async (key) => {
        value = livePositions[key];
        const positions = Object.keys(value)
        await asyncForEach(positions, async (position) => {
             const position_value = value[position]

             await redis_new.hset('positions:' + key, position, JSON.stringify(position_value))

         });
    })

        
    const liveOrders = JSON.parse(await jsonGet(redis_old, 'liveOrders', '.'))
    keys = Object.keys(liveOrders)

    await asyncForEach(keys, async (key) => {
        value = liveOrders[key];
        const positions = Object.keys(value)
        await asyncForEach(positions, async (position) => {
             const position_value = value[position]

             await redis_new.hset('orders:' + key, position, JSON.stringify(position_value))

         });
    })

    const livePositionsMainchain = JSON.parse(await jsonGet(redis_old, 'livePositionsMainchain', '.'))
    
    keys = Object.keys(livePositionsMainchain)
    await asyncForEach(keys, async (key) => {
        value = livePositionsMainchain[key];
        const positions = Object.keys(value)
        await asyncForEach(positions, async (position) => {
             const position_value = value[position]

             await redis_new.hset('mainchain_positions:' + key, position, JSON.stringify(position_value))

         });
    })

        
    const liveOrdersMainchain = JSON.parse(await jsonGet(redis_old, 'liveOrdersMainchain', '.'))
    keys = Object.keys(liveOrdersMainchain)

    await asyncForEach(keys, async (key) => {
        value = liveOrdersMainchain[key];
        const positions = Object.keys(value)
        await asyncForEach(positions, async (position) => {
             const position_value = value[position]

             await redis_new.hset('mainchain_order:' + key, position, JSON.stringify(position_value))

         });
    })

    const livePositionsTestchain = JSON.parse(await jsonGet(redis_old, 'livePositionsTestchain', '.'))
    
    keys = Object.keys(livePositionsTestchain)
    await asyncForEach(keys, async (key) => {
        value = livePositionsTestchain[key];
        const positions = Object.keys(value)
        await asyncForEach(positions, async (position) => {
             const position_value = value[position]

             await redis_new.hset('testchain_positions:' + key, position, JSON.stringify(position_value))

         });
    })

        
    const liveOrdersTestchain = JSON.parse(await jsonGet(redis_old, 'liveOrdersTestchain', '.'))
    keys = Object.keys(liveOrdersTestchain)

    await asyncForEach(keys, async (key) => {
        value = liveOrdersTestchain[key];
        const positions = Object.keys(value)
        await asyncForEach(positions, async (position) => {
             const position_value = value[position]

             await redis_new.hset('testchain_orders:' + key, position, JSON.stringify(position_value))

         });
    })


    console.log('completed - ', (Date.now() - start) / 1000, 'ms')
    return

}


async function getAllPositions(){
    return new Promise(async (resolve, reject) => {
        const allMarkets = [];

        const stream = redis_new.scanStream({
            match: "positions:*",
            count: 1000,
        });
    
        stream.on("data", async (resultKeys = []) => {
            for (let i = 0; i < resultKeys.length; i++) {
                const key = resultKeys[i].split(':')

                allMarkets.push(key[1]);
            }
        });
        stream.on("end", async () => {

            const output = {};
            for (let i = 0; i < allMarkets.length; i++) {

                output[allMarkets[i]] = {};

                const positions = await redis_new.hgetall('positions:' + allMarkets[i])
                const pos_keys = Object.keys(positions)
                for (let j = 0; j < pos_keys.length; j++) {
                    const key = pos_keys[j]
                    
                    output[allMarkets[i]][key] = JSON.parse(positions[key]);
                }

            }
            
            resolve(output);
        });
    
    })
}


const awaitAll = (promises) => {
    return new Promise(async (resolve, reject) => {
        Promise.all(promises).then(success => {
            resolve(true);
        }).catch(error => {
            Logger.error({ message: 'Error calling updates', error });
            reject(error);
        })        
    });
}

module.exports = {
    populate
}