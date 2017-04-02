
//Use this script to query demographics by hour on the dayhour collection
var config = require('./config');
var dayhour = config.dayhour;
var connection = require('./connection');
var db;

function trafficCount(opts, callback){
      var aggregateQuery =[
      //match data based on entered loc, place, start and end date and start and end hour
        {"$match":
             {"loc_id": {"$eq": opts.locID},
              "place_id": {"$eq": opts.placeID},
              "date": {"$gte": opts.startDate, "$lt": opts.endDate}
              }
         },
          //group by day by sibtracting seconds, minutes, and hours from date
         {"$project" : {
            "loc_id" : 1,
            "place_id" : 1,
            "traffic_map" : 1,
            "capture_time" : "$date",
               "h" : {
                    "$hour" : "$date"
               },
               "m" : {
                    "$minute" : "$date"
               },
               "s" : {
                    "$second" : "$date"
               },
               "ml" : {
                    "$millisecond" : "$date"
               }
            }
        },
        {"$project" : {
            "loc_id" : 1,
            "place_id" : 1,
            "traffic_map" : 1,
            "dayDate" : {
                "$subtract" : [
                        "$capture_time",
                         {
                              "$add" : [
                                   "$ml",
                                   {
                                        "$multiply" : [
                                             "$s",
                                             1000
                                        ]
                                   },
                                   {
                                        "$multiply" : [
                                             "$m",
                                             60,
                                             1000
                                        ]
                                   },
                                    {
                                        "$multiply" : [
                                             "$h",
                                             60,
                                             60,
                                             1000
                                        ]
                                   }
                              ]
                         }
                    ]
               }
            }
        },
        //group by date and sum traffic count per day
        //**may need to redo the times because of the offset
        {"$group":
             {"_id":{
              "dayDate":"$dayDate"},
              "traffic_map_day": {"$sum": "$traffic_map"}
            }
        },
        {"$project" : {
            "_id" : 0,
            "dayDate" : "$_id.dayDate",
            "traffic_map_day" : 1
          }
        }];

      db
      .collection(dayhour)
      .aggregate(aggregateQuery, {allowDiskUse:true})
      .toArray(callback);
};

//call fxn to test code
var opts = {
  locID: 8,
  placeID: 47,
  startDate: new Date(2016,0,1),
  endDate: new Date(2018, 0, 2),
  startTime: 5,
  endTime: 6
}

//create db connection
connection.connect(function(_db) {
  db = _db;

  _init();
});

function _init() {
//call function with data for testing
  trafficCount(opts, function(err, trafficResults) {
    console.log(trafficResults)
  });
};
