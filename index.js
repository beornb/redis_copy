//const redisCurrent = require("./redisCurrent/redisCurrent");
const redisNew = require("./redisNew/redisNew");
//const keydb = require("./keydb/keydb");
//const dynamodb = require("./dynamodb/dynamodb");

//const redis_arjet = new Redis(6386, '127.0.0.1', { password: 'testredis123' });



async function main(){
    //await redisCurrent.populate();
    //await redisCurrent.benchmarkExecute();
    await redisNew.populate();
    //await redisNew.benchmarkExecute();
    //await keydb.populate();
    //await keydb.benchmarkExecute();
    //await dynamodb.populate();
    //await dynamodb.benchmarkExecute();
    process.exit(0);

}

main();