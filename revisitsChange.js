//Use this script to get the increase/decrease in revisits
var config = require('./config');
var duration = config.duration;
var lodash = require('lodash');
var connection = require('./connection');
var db;

  function repeatNewChange(queryOptions, callback){
    var aggregateQuery =[{
    //match on loc_id, place_id, and capture time
    "$match":{
      "loc_id": queryOptions.locID,
      "place_id": queryOptions.placeID,
      "capture_time":{"$gt":queryOptions.startDate,
      "$lt": queryOptions.endDate}
    }
  }, {
    //remove the mac ids from the array they were in
    //change capture time into day of year
    "$project":{
      "mac":"$array.mac",
      "dayOfYear":{"$dayOfYear":"$capture_time"}
    }
  },
    //each mac id is now associated with one day of the year in one document
    {"$unwind":"$mac"},
    //group by mac id and day of year so that mac ids on the same day are not counted more than once
    {
      "$group":{
        "_id":{"mac":"$mac", "dayOfYear":"$dayOfYear"}
      }
    },
    //count how many times a customer visits
    {
      "$group":{
        "_id":{"mac":"$_id.mac"},
        "count": { "$sum": 1 }
      }
    },
    ////classify visits as first time or repeat if mac has visited more than one time on different days
    {
      "$project":
      {
        "_id": 1,
        "revisits":{
          "$cond":{
            "if": { "$gte": ["$count", 2] },
            "then": "repeat",
            "else": "first_time"
          }
        }
      }
    },
    //group revisits and count
    {
      "$group":{
        "_id":"$revisits",
        "countOfVisits": { "$sum": 1 }
      }
    }];
    db
    .collection(duration)
    .aggregate(aggregateQuery, {allowDiskUse:true})
    .toArray(callback)
  };

  function repeatChangeCall(queryBefore, queryAfter) {
    repeatNewChange(queryBefore, function(err, repeatOne) {
      var resultOne = repeatOne.filter(function (entry) { return entry._id === "repeat"; });
      var resultFirst = lodash.map(resultOne, 'countOfVisits')[0];
      repeatNewChange(queryAfter, function(err, repeatTwo) {
        var resultTwo = repeatTwo.filter(function (entry) { return entry._id === "repeat"; });
        var resultSecond = lodash.map(resultTwo, 'countOfVisits')[0];
        var change = (resultSecond-resultFirst)/resultFirst;
        console.log(change);
        return(change);
      });
    });
  };

  function newChangeCall(queryBefore, queryAfter) {
    repeatNewChange(queryBefore, function(err, repeatOne) {
      var resultOne = repeatOne.filter(function (entry) { return entry._id === "first_time"; });
      var resultFirst = lodash.map(resultOne, 'countOfVisits')[0];
      repeatNewChange(queryAfter, function(err, repeatTwo) {
        var resultTwo = repeatTwo.filter(function (entry) { return entry._id === "first_time"; });
        var resultSecond = lodash.map(resultTwo, 'countOfVisits')[0];
        var change = (resultSecond-resultFirst)/resultFirst;
        console.log(change);
        return(change);
      });
    });
  };

//define parameter options
var opts = {
  locID: 8,
  placeID: 47,
  startOne: new Date(2016,0,1),
  endOne: new Date(2017, 0, 2),
  startTwo: new Date(2017,0,1),
  endTwo: new Date(2018, 0, 2)
};

//create queryBefore and queryAfter variables so that
//two different date ranges are applied to function
var queryBefore = lodash.merge({}, opts, {
  startDate: opts.startOne,
  endDate: opts.endOne
});

var queryAfter = lodash.merge({}, opts, {
  startDate: opts.startTwo,
  endDate: opts.endTwo
});

//establish connection
connection.connect(function(_db) {
  db = _db;

  _init();
});

//call queries
function _init() {
  repeatChangeCall(queryBefore, queryAfter);
  newChangeCall(queryBefore, queryAfter);
};
