var throttledQueue = require('throttled-queue');
const fs = require('fs');
const axios = require('axios');
const CsvReadableStream = require('csv-reader');
const path = require('path');
const AutoDetectDecoderStream = require('autodetect-decoder-stream');

let count = 0;
let totalRows = 0;
let currentFile = 1; 
const fileNamePrefix = "d_";
const logCompletedRows = 10;
const requestsPerSecond = 5;

var throttle = throttledQueue(requestsPerSecond, 1000); 


const updateInfo = () => {

    if(count % logCompletedRows === 0) console.log(`API iterated ${count} rows`);
    if(count >= totalRows) {
        console.log("Completed all rows");
        count = 0;
        totalRows = 0;
        currentFile ++;
        start(currentFile);
        console.log(`Starting next file: ${fileNamePrefix}${currentFile}.csv`);
    }
};

const logErrors = (rowData, url) => {

    console.log("FAILED", rowData);

    fs.appendFile('log.txt', url + "\n", function (err) {
        if (err) {
            console.log("CurrentFile:", currentFile)
            console.log("FILE WRITE ERROR:", err);
          } else {
              // done
          }
      });
}

const updateChannel = (rowData) => {

    const url = rowData[0]
    var data = JSON.stringify({"allow_auto_unhide":false,"should_hide_all":true});
    const urlString = `https://api-SENDBIRD_APP_ID.sendbird.com/v3/group_channels/${url}/hide`
    var config = {
      method: 'put',
      url: urlString,
      headers: { 
        'Api-Token': 'SENDBIRD_API_TOKEN', 
        'Content-Type': 'application/json'
      },
      data : data
    };
    axios(config)
    .then(function (response) { 
        count ++;
        updateInfo();
    })
    .catch(function (error) {
      count ++;
      updateInfo();
      const errorMessage =  error.toJSON().message;
      if(errorMessage) logErrors(rowData, url);
   
    });
}


const start =(currentFile) => {

    let file = null;

    try {
        file = path.resolve(`${fileNamePrefix}${currentFile}.csv`);
    } catch (e) {
        console.log(`file not found: ${fileNamePrefix}${currentFile}.csv`)
        return;
    }

    let inputStream = fs.createReadStream(file)
    .pipe(new AutoDetectDecoderStream({
        defaultEncoding: '1255'
    })); 

    inputStream
        .pipe(new CsvReadableStream({
            parseNumbers: true,
            parseBooleans: true,
            trim: true
        }))
        .on('data', function (row) {
            totalRows ++;

            //Call Endpoint for current file. 
            //Row data

            //Throttle to the endpoint of your choice.
            throttle(() => updateChannel(row));

        }).on('end', function (data) {
            //Completed fetching data from CSV file.
            console.log(`Fetched all rows from csv file into memory! ${totalRows} rows in memory.`);
        }); 
}

start(currentFile)


