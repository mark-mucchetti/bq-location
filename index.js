const glob = require('glob-promise');
const fs = require('fs');
const { createGzip } = require('zlib');
const { promisify } = require('util');
const { pipeline } = require('stream');
const mapSemantic = require('./map-semantic.js');
const { Storage } = require('@google-cloud/storage');
const { BigQuery } = require('@google-cloud/bigquery');
const colors = require('colors');
const bigquery = new BigQuery();
const storage = new Storage();

// you'll need your own GCP creds to go here
process.env.GOOGLE_APPLICATION_CREDENTIALS = "./service-account-key.json";

// wildcard path for where to find semantic location data in takeout format
const sourceLocation = './sem/**/*.json';

// the default timezone, when a location cannot be found
const default_tz = "America/Los_Angeles";
const defaultLocation = "US";

// the GCS bucket to stage the file
const bucket = "mark-location-history";
const newfile = 'semantic-history.jsonl';

// the BigQuery dataset and table to load the file
const dataset = 'location';
const table = 'semantic_history';

// the schema for the location table (must match the output in map-semantic.js)
const schemaFile = './schema.json';
const schema = JSON.parse(fs.readFileSync(schemaFile));

/// main method
/// 
/// This method processes an input folder, loads the results to GCS,
/// and then loads the JSONL file to BigQuery. This is the controller
/// method for the whole process. If you want to consume it as a library
/// then convert it to a standard export function and reference it from
/// elsewhere.
(async () => {

    console.log(`Rewriting semantic location history as ${newfile}...`.gray);
    await processFiles(newfile);

    // NOTE: I can't actually find a way to make the SDK accept a compressed file from GCS.
    //console.log(`Zipping file...`.gray);
    //let outfile = await gzip(newfile);
    let outfile = newfile;
    
    console.log(`Uploading file to ${bucket} in GCS...`.gray);
    let gcsFile = await uploadToGCS(outfile, bucket);

    // make BQ table
    await createBQTable(dataset, table);

    console.log(`Transferring GCS file at ${gcsFile} to BigQuery...`.gray);
    let jobid = await loadToBQ(dataset, table, bucket, outfile);

    console.log(`Post-processing...`.gray);
    await postProcess(dataset, table);

    console.log(`Load complete as job ${jobid}.`.gray);
    console.log(`Finished!`.yellow);

})();

/// processFiles
///
/// Looks for all JSON files in the specified wildcard glob and 
/// converts them into JSONL format for BigQuery to read.
async function processFiles(newfile)
{
    let stream = fs.createWriteStream(newfile);
    let files = await glob(sourceLocation);

    for (filename of files)
    {  
        let jsonr = fs.readFileSync(filename);
        await mapSemantic.mapToStream(stream, jsonr, default_tz);
    }

    stream.end();
}

/// gzip
///
/// Gzips an input file. NOTE: I couldn't actually find a way
/// to specify gzipped files to the nodejs BigQuery SDK, so this is
/// currently dormant. If you know a way, please let me know.
async function gzip(newfile)
{
    const pipe = promisify(pipeline);
    const outfile = `${newfile}.gz`;

    const gzip = createGzip();
    const source = fs.createReadStream(newfile);
    const destination = fs.createWriteStream(outfile);
    await pipe(source, gzip, destination);

    return outfile;
}

/// uploadToGCS
///
/// takes a filename and uploads it to a GCS bucket.
async function uploadToGCS(newfile, bucket)
{

    await storage.bucket(bucket).upload(newfile, {
        gzip: true,
    });

    return `gs://${bucket}/${newfile}`;

}

/// createBQTable
///
/// makes the appropriate table for the semantic location
/// data transformation. For testing purposes, this function
/// will delete the existing table if it finds it and recreates it.
/// For an incremental process where you load new data each month,
/// you would want to change this.
async function createBQTable(dataset, table)
{
    let [exist] = await bigquery.dataset(dataset).table(table).exists();

    if (exist)
    {
        console.log(`Deleting existing BigQuery table...`.gray)

        // delete the table and recreate it
        await bigquery.dataset(dataset).table(table).delete();
    }

    const options = {
      schema: schema,
      timePartitioning: {
        type: 'DAY',
        field: 'date',
      },
    };

    console.log(`Creating new BigQuery table ${dataset}.${table}...`.gray);
    const [result] = await bigquery.dataset(dataset).createTable(table, options);

    return result;

}

/// loadToBQ
///
/// Takes the GCS file and loads it into BigQuery, into the table
/// created by the previous method. No schema needs to be specified here
/// since we already defined it in the creation of the table.
///
/// You can do both of those steps together, but again for testing
/// reasons I wanted to separate them.
async function loadToBQ(dataset, table, bucket, file)
{
    const metadata = {
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        location: defaultLocation,
        
    };

    const [job] = await bigquery.dataset(dataset).table(table)
    .load(storage.bucket(bucket).file(file), metadata);

    const errors = job.status.errors;
    if (errors && errors.length > 0)
    {
        throw errors;
    }
    
    return job.id;
}

/// postProcess
///
/// Doesn't do anything yet. Should run queries to fix missing data
/// where appropriate.
async function postProcess(dataset, table)
{

}

