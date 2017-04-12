//query on duration collection to get average duration and numbers of revisits
var config = require('./config');
var duration = config.duration;
var connection = require('./connection');
var db;

    function avgDuration(opts, callback){
        var aggregateQuery =[
        //match data based on location, palce, and a date range
        {"$match":{
            "loc_id": opts.locID,
            "place_id": opts.placeID,
            "capture_time":{
               "$gte": opts.start,
               "$lt": opts.end}
            }
        },
        //unwind the array so that each mac id can be regrouped outside of the 15 minute
        // intervals and the duration can be summed
        {"$unwind":"$array"},
        {"$project":{
            "mac":"$array.mac",
            "duration":"$array.duration"
            }
        },
        //remove mac ids and durations that are zero seconds long
        {"$match":{
            "duration":{"$gt":0}
            }
        },
        //group by mac id and sum the durations across different intervals.
        //this will give the total time a mac is present (minus a few seconds becuase of the interval split)
        {"$group":{
            "_id":{
            "mac":"$mac"},
            "duration_sum":{"$sum":"$duration"}
            }
        },
        //average the total duration over all of the mac addresses selected
        {"$group":{"_id":"$item",
            "duration_avg":{"$avg": "$duration_sum" }
            }
        }];

    db
      .collection(duration)
      .aggregate(aggregateQuery, {allowDiskUse:true})
      .toArray(callback)
};

//query for repeat visitors
function repeatVisit(opts, callback){
    var aggregateQuery =[
    //match on loc_id, place_id, and capture time
    {"$match":{
        "loc_id" : opts.locID,
        "place_id" : opts.placeID,
        "capture_time":{"$gt" : opts.start,
                    "$lt" : opts.end}
        }
    },
    //remove the mac ids from the array they were in
    //change capture time into day of year
    {"$project":{
        "mac":"$array.mac",
        "dayOfYear":{"$dayOfYear":"$capture_time"}
        }
    },
    //each mac id is now associated with one day of the year in one document
    {"$unwind":"$mac"},
    //group by mac id and day of year so that mac ids on the same day are not counted more than once
    {"$group":{
        "_id":{"mac":"$mac", "dayOfYear":"$dayOfYear"}
        }
    },
    //count how many times a customer visits
    {"$group":{
        "_id":{"mac":"$_id.mac"},
         "count": { "$sum": 1 }
        }
    },
    //count the count of customer visits for each number of visitor occurences (ex: 3 people visited 4 times)
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

//create option parameters for functions
var opts = {
locID: 8,
placeID: 47,
start: new Date(2016,0,16),
end: new Date(2017,0,16),
}

connection.connect(function(_db) {
  db = _db;

  _init();
});

function _init() {
  avgDuration(opts, function(err, avgDuration){
    console.log(avgDuration);
  });
  repeatVisit(opts, function(err, repeats){
    console.log(repeats);
  });
}

