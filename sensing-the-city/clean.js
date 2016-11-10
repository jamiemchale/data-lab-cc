var csv = require('csv');
var fs = require('fs');
var moment = require('moment');

var stream = fs.createReadStream(__dirname + '/airquality.csv');
var outputFile = fs.createWriteStream('airquality_clean.csv');

function airQuality(record) {
  var nodeId = record[1];
  var time = record[8].substr(0,10);
  var lat = record[6];
  var lng = record[7];

  if (nodeId === "1" || nodeId === "2") {
    return null;
  }

  if (lat === '0' || lng === '0') {
    return null;
  }

  if (time === '0' || moment(time).isBefore('2016-05-15') ) {
    return null;
  }

  if (nodeId === '6' || nodeId === '4' && moment(time).isAfter('2016-06-13')) {
    return null;
  }

  return record;
}



stream
  .pipe(csv.parse())
  .pipe(csv.transform(airQuality))
  .pipe(csv.stringify())
  // .pipe(process.stdout);
  .pipe(outputFile);


/*
  ✅ * Disregard “SensorNodeId” 1 and 2.
  ✅ * Disregard data prior to 15-May-16 (start of deployment trial).
  ✅ * Disregard “SensorNodeId” 6 after 13-Jun-16.
  ✅ * Disregard “SensorNodeId” 4 after 13-Jun-16.
  ✅ * Disregard “Latitude” and “Longitude” values of 0.
  ✅ * Disregard “Time” values of 0.
*/
