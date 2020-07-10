# bq-location

This library takes a JSON location history file downloaded from [Google Takeout](https://takeout.google.com/settings/takeout) and loads it to BigQuery.

Prerequisites:

- A service account key for Google Cloud Platform, with permission to Google Cloud Storage and BigQuery
- A JSON location file exported from Google Takeout with a folder called "Semantic Location History"
- The APIs for Google Cloud Storage and BigQuery enabled inside your GCP project

All of the variables are hardcoded in the index.js script as follows:

```// you'll need your own GCP creds to go here
process.env.GOOGLE_APPLICATION_CREDENTIALS

// wildcard path for where to find semantic location data in takeout format
const sourceLocation

// the default timezone, when a location cannot be found
const default_tz

// default location for BigQuery processing
const defaultLocation

// the GCS bucket and filename to stage the file
const bucket
const newfile

// the BigQuery dataset and table to load the file
const dataset = 'location';
const table = 'semantic_history'
```

Once everything is configured, you can run the script with no arguments:

``` node ./index.js```

It should produce output like the following.

```Rewriting semantic location history as semantic-history.jsonl...
Uploading file to location-history-bucket in GCS...
Deleting existing BigQuery table...
Creating new BigQuery table location.semantic_history...
Transferring GCS file at gs://location-history-bucket/semantic-history.jsonl to BigQuery...
Post-processing...
Load complete as job bort-project:US.1dcff2cd-076f-47df-8c08-1deadbeefac1.
Finished!
```

This is a pretty barebones version but easily modifiable to your own needs. I don't currently have any intention of further maintaining it, but who knows.

Accompanying blog post describing the process at [https://virtu.is/google-maps-semantic-history-in-bigquery/](https://virtu.is/google-maps-semantic-history-in-bigquery/).