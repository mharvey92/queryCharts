//query duration collection to retun average duration by type of visit (repeat vs. first time)
var config = require('./config');
var duration = config.duration;
var connection = require('./connection');
var db;

    function avgDurationRevisit(opts, callback){
        var aggregateQuery =[
        {"$match":{
            "loc_id" : opts.locID,
            "place_id" : opts.placeID,
            "capture_time":{"$gt" : opts.startDate,
                        "$lt" : opts.endDate}
            }
        },
        {"$unwind":"$array"},
        //group mac id per day, and sum the duration over the day
        {"$group" : {
            "_id":{
            "mac":"$array.mac",
            "dayOfYear":{"$dayOfYear":"$capture_time"}},
            "duration": {"$sum" : "$array.duration"}
            }
        },
        //get rid of durations that are zero time
        {"$match":{
            "duration":{"$gt":0}
            }
        },
        //count how many times a mac id shows up on different days, push the durations to an array
        {"$group":{
            "_id":{"mac":"$_id.mac"},
             "count": { "$sum": 1 },
             "duration" : {"$push":{"duration":"$duration"}
                }
            }
        },
        //sum the total duration for the mac id to get the total time spent in store over selected period
        //if the mac id shows up more than once in the period selected classify it as a repeat, otherwise first time
        {"$project":
          {"_id": 1,
          "duration":{"$sum":"$duration.duration"},
            "revisits":{
              "$cond":{
                "if": { "$gte": ["$count", 2] },
                "then": "repeat",
                "else": "first_time"
              }
            }
          }
        },
        //group documents by type of revisit (repeat/first time) and calculate the average duration time
        {"$group":{
            "_id": {"revisits":"$revisits"},
            "avg_duration": {"$avg":"$duration"}
            }
        }];

      db
      .collection(duration)
      .aggregate(aggregateQuery, {allowDiskUse:true})
      .toArray(callback);
    };

//create option parameters for functions
var opts = {
locID: 8,
placeID: 47,
startDate: new Date(2016,0,16),
endDate: new Date(2018,0,16),
}

connection.connect(function(_db) {
  db = _db;

  _init();
});

function _init() {
  avgDurationRevisit(opts, function(err, result){
    console.log(result);
  });
};
