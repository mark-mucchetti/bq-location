const fs = require('fs')
const filename = './lh.json'

let jsonr = fs.readFileSync(filename);
let jp = JSON.parse(jsonr);
let array = jp.locations;

let newfile = filename.replace(/.json$/g, '.jsonl')
let stream = fs.createWriteStream(newfile);
array.map(x =>
    { 
        let y = { timestamp: parseInt(x.timestampMs / 1000),
                  lat: x.latitudeE7 / 10000000,
                  lon: x.longitudeE7 / 10000000,
                  accuracy: x.accuracy };
        //console.log(JSON.stringify(y));
        stream.write(`${JSON.stringify(y)}\n`)
    
});
stream.end();