//Run query to transfer documents from trafficmap_all to dayhour collection
//This query counts the number genz, millennials etc. by hour and gender
var config = require('./config');
var dayhour = config.dayhour;
var demographics = config.demographics;
var connection = require('./connection');
var db;

function _getDemoData(startDate, callback){
    var matchGroup = [{
        //match on specified date
        "$match": {
              "capture_time" : {
              "$gte": startDate
                      }
            }
        },
        //group by u_id, loc, place, average age, sum of gender (specified as 1,-1, 0), and the minimum capture time in order to
        //create a unique document with each users demographic information
        {"$group" :
            { "_id" : "$u_id",
            "loc_id": {"$first": "$loc_id"},
            "place_id": {"$first":"$place_id"},
            "age": {"$avg": "$age"},
            "gender": { "$sum": "$gender" },
            "capture_time":{"$min": "$capture_time"}
            }
        }];

    var genderCategory = [{
        //project all fields and designate each u_id as either male, female, or unknown
        "$project":
            {
            "_id" : 1,
            "loc_id": 1,
            "place_id": 1,
            "age":1,
            "capture_time":1,
            "gender_category":{
                "$cond": {
                    "if": { "$gte": [ "$gender", 1 ] },
                    "then": "male",
                    "else": {
                        "$cond":{
                            "if": { "$lte": ["$gender",-1]},
                            "then": "female",
                            "else": "unknown"
                            }
                        }
                    }
                }
            }
        }];

    var ageCategory = [{
        //project all fields and designate each average age to an age category (e.g. genz, millennials, genx)
        "$project":
            {
            "_id": 1,
            "loc_id":1,
            "place_id":1,
            "capture_time":1,
            "gender_category":1,
            "age_category":{
                "$cond":{
                    "if": { "$lte": ["$age", 18] },
                    "then": "genz",
                    "else": {
                        "$cond":{
                            "if":{
                                "$and":[
                                        {"$gt": ["$age", 18] },
                                        {"$lte": [ "$age", 34]}
                                        ]
                                    },
                                    "then":"millennials",
                            "else": {
                                "$cond":{
                                    "if":{
                                        "$and":[
                                                {"$gt": ["$age", 34] },
                                                {"$lte": [ "$age", 50]}
                                                ]
                                        },
                                        "then":"genx",
                                    "else": {
                                        "$cond":{
                                            "if":{
                                                "$and":[
                                                    {"$gt": ["$age", 50] },
                                                    {"$lte": [ "$age", 69]}
                                                        ]
                                            },
                                            "then":"boomer",
                                            "else": {
                                                "$cond":{
                                                    "if":{"$gte": ["$age", 69]},
                                                    "then":"silent",
                                                    "else":"unknown"}
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }];

    var hours = [{
        //project all fields and split up capture time into hours, minutes, seconds, and milliseconds
        "$project" :
            {
            "_id": 1,
            "loc_id":1,
            "place_id":1,
            "gender_category":1,
            "age_category":1,
            "capture_time" : "$capture_time",
               "h" : {
                    "$hour" : "$capture_time"
               },
               "m" : {
                    "$minute" : "$capture_time"
               },
               "s" : {
                    "$second" : "$capture_time"
               },
               "ml" : {
                    "$millisecond" : "$capture_time"
               }
            }
        },
        //project all fields and subtract the milliseconds and minutes so that we can group by hour
        {"$project" : {
            "_id": 1,
            "loc_id":1,
            "place_id":1,
            "gender_category":1,
            "age_category":1,
               "by_day_hour" : {
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
                                   }
                              ]
                         }
                    ]
               }
            }
        }];

    var ageCount = [{
        //count the number of males, females, unknowns, genz, millennials etc for each hour
        "$group" : {
                "_id" : {
                "date" : "$by_day_hour",
                "loc_id" : "$loc_id",
                "place_id" : "$place_id",
                "gender" : "$gender_category"},
        				"genz" :  {
        					"$sum" : {
        						"$cond" : { "if": { "$eq": ["$age_category", "genz"]}, "then": 1, "else": 0}
        					}
        				},
        				"millennials" :  {
        					"$sum" : {
        						"$cond" : { "if": { "$eq": ["$age_category", "millennials"]}, "then": 1, "else": 0}
        					}
        				},
        				"genx" :  {
        					"$sum" : {
        						"$cond" : { "if": { "$eq": ["$age_category", "genx"]}, "then": 1, "else": 0}
        					}
        				},
        				"boomer" :  {
        					"$sum" : {
        						"$cond" : { "if": { "$eq": ["$age_category", "boomer"]}, "then": 1, "else": 0}
        					}
        				},
                        "silent" :  {
        					"$sum" : {
        						"$cond" : { "if": { "$eq": ["$age_category", "silent"]}, "then": 1, "else": 0}
              }
        		}
          }
        },
        //push age by gender to object
        {"$group" : {
            "_id" : {
                "date" : "$_id.date",
                "loc_id" : "$_id.loc_id",
                "place_id" : "$_id.place_id"},
                "demographics" : {"$push" :{
                    "gender" : "$_id.gender",
                    "genz" : "$genz",
                    "millennials" : "$millennials",
                    "genx" : "$genx",
                    "boomer" : "$boomer",
                    "silent" : "$silent"}}
            }
        },
        //project _id field so that we don't have to match on long fields (ex: $_id.place_id)
        {"$project" : {
            "date" : "$_id.date",
            "loc_id" : "$_id.loc_id",
            "place_id" : "$_id.place_id",
            "hour": {"$hour": "$_id.date"},
            "demographics" : 1,
            "_id" : 0
          }
        }];

     db
    .collection(demographics)
    .aggregate([].concat(matchGroup, genderCategory, ageCategory, hours, ageCount), {allowDiskUse:true})
    .toArray(callback)
  };

function _update_db(result) {
  //update the database with demo records
  //check if a document already exists for that loc, place, date

  var collection = db.collection(dayhour);
  collection.find({
    date: result.date,
    loc_id: result.loc_id,
    place_id: result.place_id})
  .toArray(function(err, doc){
    if (err) {
      console.log(err);
    };
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
        { "$set": {"demographics": result.demographics}},
        {upsert:true}, function(error) {
          if (error) {
            console.log(error);
          };
        });
    };
  });
};

/**
   * Requires Callback!
   **/
   //pull the last date where a demographics field exists (e.g. male)
   function _get_start_date(callback){
    if (!callback) {
      throw new Error("Requires a callback!");
    }
    var collection_dayhour = db.collection(dayhour);
    collection_dayhour
    .find({'male':{'$exists':true}})
    .sort({"date": -1})
    .limit(1)
    .toArray(function(err, doc){
      if (err) {
        callback(err);
      }
      console.log(doc)
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
    _getDemoData(startDate, function(err, demoData) {
      for(var i = 0; i < demoData.length; i++){
       _update_db(demoData[i]);
   };
      //db.close();
    });
  });
};
