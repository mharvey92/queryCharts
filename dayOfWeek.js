var config = require('./config');
var dayhour = config.dayhour;
var demographics = config.demographics;
var connection = require('./connection');
var db;

function dayOfWeekAge(opts, callback){
    var aggregateQuery = [
    {"$match":
             {"loc_id": {"$eq": opts.locID},
              "place_id": {"$eq": opts.placeID},
              "date": {"$gte": opts.startDate, "$lt": opts.endDate}
            }
    },
    {"$unwind" : "$demographics"},
    //classify the day of week each document occurs
    {"$project":
        {
            "_id": 1,
            "demographics":1,
            "dayOfWeek": { "$dayOfWeek" : "$date" },
        }
    },
    {"$project":
        {
            "_id":1,
            "demographics":1,
            "dayOfWeek" : {
                "$cond" :{
                    "if" :{
                        "$and":[
                        {"$gt": ["$dayOfWeek", 1] },
                        {"$lte": [ "$dayOfWeek", 6]}
                        ]
                    },
                    "then":"weekday",
                    "else":"weekend"

                }
            }
        }
    },
    {"$group" : {
        "_id" : {"dayOfWeek" : "$dayOfWeek"},
        "Genz" : {"$sum" : "$demographics.genz"},
        "Mill" : {"$sum" : "$demographics.millennials"},
        "Genx" : {"$sum" : "$demographics.genx"},
        "Boom" : {"$sum" : "$demographics.boomer"},
        "Silent" : {"$sum" : "$demographics.silent"}
        }
    }];

    db
    .collection(dayhour)
    .aggregate(aggregateQuery, {allowDiskUse:true})
    .toArray(callback)
};

function dayOfWeekGender(opts, gender, callback){
  var aggregateQuery = [
    {"$match":
             {"loc_id": {"$eq": opts.locID},
              "place_id": {"$eq": opts.placeID},
              "date": {"$gte": opts.startDate, "$lt": opts.endDate}
            }
    },
    {"$unwind" : "$demographics"},
    {"$match": {
          "demographics.gender": gender
      }
    },
    //classify the day of week each document occurs
    {"$project":
        {
          "_id": 1,
          "demographics":1,
          "dayOfWeek": { "$dayOfWeek" : "$date" },
        }
    },
    {"$project":
        {
          "_id":1,
          "totalSum": { '$add' : [ '$demographics.genz', '$demographics.millennials', '$demographics.genx', '$demographics.boomer', "$demographics.silent" ] },
          "dayOfWeek" : {
              "$cond" :{
                 "if" :{
                    "$and":[
                      {"$gt": ["$dayOfWeek", 1] },
                      {"$lte": [ "$dayOfWeek", 6]}
                        ]
                      },
                      "then":"weekday",
                      "else":"weekend"
                }
            }
        }
    },
    {"$group" : {
        "_id" : {"dayOfWeek" : "$dayOfWeek"},
        "gender" : {"$sum" : "$totalSum"}
        }
    }];

    db
    .collection(dayhour)
    .aggregate(aggregateQuery, {allowDiskUse:true})
    .toArray(callback)
};


var opts = {
  locID: 8,
  placeID: 47,
  startDate: new Date(2016,0,1),
  endDate: new Date(2016,12,10),
};

connection.connect(function(_db) {
  db = _db;

  _init();
});

var optsArray = ['male', 'female', 'unknown'];

function _init() {
  dayOfWeekAge(opts, function(err, dayOfWeek){
    console.log(dayOfWeek);
  });

optsArray.forEach(function(gender){
    dayOfWeekGender(opts, gender, console.log);
  });

};
