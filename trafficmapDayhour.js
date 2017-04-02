//Run query to transfer documents from trafficmap_all to dayhour collection
//This query counts unique mac ids by the hour they first show up
//In other words, if a cell phone remains for multiple hours they will not be counted again that day
//We are doing this so that the totals when aggregated to days instead of hours are not inflated.
//Currently there are no parameters to exclude mac ids that pass by quickly or stay for a long time (maybe employees).

var config = require('./config');
var dayhour = config.dayhour;
var traffic_map = config.traffic_map;
var connection = require('./connection');
var db;

function _getTrafficData(startDate, callback) {
  var aggregateQuery = [{
      "$match": {
        "capture_time": {
          "$gte": startDate
        }
      }
    },
    {
      "$group": {
        "_id": {
          "mac": "$mac",
          "loc_id": "$loc_id",
          "place_id": "$place_id"
        },
        "min_capture_time": {
          "$min": "$capture_time"
        }
      }
    },
    {
      "$project": {
        "loc_id": "$_id.loc_id",
        "place_id": "$_id.place_id",
        "min_capture_time": "$min_capture_time",
        "_id": 0,
        "h": {
          "$hour": "$min_capture_time"
        },
        "m": {
          "$minute": "$min_capture_time"
        },
        "s": {
          "$second": "$min_capture_time"
        },
        "ml": {
          "$millisecond": "$min_capture_time"
        }
      }
    },
    {
      "$project": {
        "loc_id": 1,
        "place_id": 1,
        "by_day_hour": {
          "$subtract": [
          "$min_capture_time",
          {
            "$add": [
            "$ml",
            {
              "$multiply": [
              "$s",
              1000
              ]
            },
            {
              "$multiply": [
              "$m",
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
    {
      "$group": {
        "_id": {
          "date": "$by_day_hour",
          "loc_id": "$loc_id",
          "place_id": "$place_id"
        },
        "traffic_map": {
          "$sum": 1
        }
      }
    },
    {
      "$project": {
        "_id": 0,
        "date": "$_id.date",
        "loc_id": "$_id.loc_id",
        "place_id": "$_id.place_id",
        "hour": {
          "$hour": "$_id.date"
        },
        "traffic_map": 1
      }
    }];

    db
    .collection(traffic_map)
    .aggregate(aggregateQuery, {allowDiskUse:true})
    .toArray(callback)
  };

  function _update_db(result) {
  //check if a document already exists for that loc, place, date
  var collection = db.collection(dayhour);
  collection.find({
    date: result.date,
    loc_id: result.loc_id,
    place_id: result.place_id})
  .toArray(function(err, doc){
    if (err) {
      console.log(err);
    }
    if (doc==""){
      console.log("create document for the first time")
      db
      .collection(dayhour)
      .insert(result, function(error) {
        if (error) {
          console.log(error);
        };
      });
    }else{
      console.log("document info already exists...we're going to update")
      db
      .collection(dayhour)
      .updateOne({ "date" : result.date,
        "loc_id" : result.loc_id,
        "place_id" : result.place_id},
        {"$set": {"traffic_map": result.traffic_map}},
        {upsert:true}, function(error) {
          if (error) {
            console.log(error);
          };
        });
    };
    //});
  });
};

  /**
   * Requires Callback!
   **/
   function _get_start_date(callback){
    if (!callback) {
      throw new Error("Requires a callback!");
    }
    var collection_dayhour = db.collection(dayhour);
    collection_dayhour
    .find({'traffic_map':{'$exists':true}})
    .sort({"date": -1})
    .limit(1)
    .toArray(function(err, doc){
      if (err) {
        callback(err);
      }
      if (doc==""){
        var year = new Date().getFullYear();
        var start = new Date(year,0,1)
        //var start = date.toISOString();
        console.log(start);
        console.log("there were no docs");
      } else {
        var start = doc[0]['date'];
        //var start = date.toISOString();
        console.log(start);
        console.log("found some docs");
      };

      callback(null, start);
    });
  };

//create db connection
connection.connect(function(_db) {
  db = _db;

  _init();
});

//call scripts functions
function _init() {
    _get_start_date(function(err, startDate) {
    _getTrafficData(startDate, function(err, trafficData) {
      for(var i = 0; i < trafficData.length; i++){
        _update_db(trafficData[i]);
      };

      //db.close();
    })
  });
};

