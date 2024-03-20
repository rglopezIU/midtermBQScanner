// import pkg for google cloud
const {Storage} = require('@google-cloud/storage');
const fs = require('fs-extra');
const csv = require('csv-parser');
const path = require('path');
const {BigQuery} = require('@google-cloud/bigquery');

const bigquery = new BigQuery();
const datasetId = 'midtermMalware';
const tableId = 'malwarePDF';


exports.readCSV = (file, context) => {
  const gcs = new Storage();
  const dataFile = gcs.bucket(file.bucket).file(file.name);

  const rows = [];

  dataFile.createReadStream()
    .pipe(csv())
    .on('data', (row) => {
      // Push each row to the array
      rows.push(row);
    })
    .on('end', () => {
      // Insert all rows into BigQuery
      writeToBQ(rows);
    })
    .on('error', (error) => {
      console.error('Error reading CSV:', error);
    });
}

async function writeToBQ(rows) {
  try {
    // Insert data into the malwarePDF table
    await bigquery
      .dataset(datasetId)
      .table(tableId)
      .insert(rows);
    console.log('Data loaded into BigQuery');
  } catch (error) {
    console.error('Error loading data into BigQuery:', error);
  }
}