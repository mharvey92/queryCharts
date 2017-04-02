//Use this script to query demographics by hour on the dayhour collection
var connection = require('./connection');
var config = require('./config');
var dayhour = config.dayhour;
var db;
var lodash = require('lodash')
var math = require('mathjs');

function genderCount(opts, gender, callback){
      //get female/male/unknowns sum
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
    {"$project" : {
        'totalSum' : { '$add' : [ '$demographics.genz', '$demographics.millennials', '$demographics.genx', '$demographics.boomer', "$demographics.silent" ] }
        }
      },
    {"$group" : {
        "_id" : gender,
        "gender" : {"$sum" : "$totalSum"}
        }
    }];

     db
      .collection(dayhour)
      .aggregate(aggregateQuery, {allowDiskUse:true})
      .toArray(callback)
};

//puts gender output into one array


function ageCount(opts, callback){
//get females/males/unknowns by age groups
     var aggregateQuery = [
      {"$match":
             {"loc_id": {"$eq": opts.locID},
              "place_id": {"$eq": opts.placeID},
              "date": {"$gte": opts.startDate, "$lt": opts.endDate}
            }
      },
      {"$unwind" : "$demographics"},
      {"$group" : {
        "_id" : "",
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

function genderAgeCount(opts, gender, callback){
//get females/males/unknowns by age groups
     var aggregateQuery = [
     {"$match":
             {"loc_id": {"$eq": opts.locID},
              "place_id": {"$eq": opts.placeID},
              "date": {"$gte": opts.startDate, "$lt": opts.endDate}
            }
      },
     {"$unwind" : "$demographics"},
     {"$match" : {
            "demographics.gender": gender
        }
    },
    {"$group" : {
        "_id" : "",
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

    function ageChange(queryBefore, queryAfter, callback){
      var ages = ['Genz', 'Mill', 'Genx', 'Boom', 'Silent'];
      results = [];
      //calls function with first set of dates and extracts values into new array
      ageCount(queryBefore, function(err, ageCountOne){
      //calls function with second set of dates and extracts values into new array
      ageCount(queryAfter, function(err, ageCountTwo){
        ages.forEach(function(age){
          var resultOne = lodash.map(ageCountOne, age)[0];
          var resultTwo = lodash.map(ageCountTwo, age)[0];
          results.push((resultTwo-resultOne)/resultOne);
      });
        callback(null, results);
        });
      });
    };

    function genderChange(queryBefore, queryAfter, callback){
      var male = 'male';
      var female = 'female';
      var unknown = 'unknown';
      resultsGender = [];

      //calls function with first set of dates
      genderCount(queryBefore, male, function(err, genMale){
        var maleResultOne = lodash.map(genMale, 'gender')[0];
      genderCount(queryBefore, female, function(err, genFemale){
        var femaleResultOne = lodash.map(genFemale, 'gender')[0];
      genderCount(queryBefore, unknown, function(err, genUnknown){
        var unknownResultOne = lodash.map(genUnknown, 'gender')[0];
      //calls function with second set of dates
      genderCount(queryAfter, male, function(err, genMale){
        var maleResultTwo = lodash.map(genMale, 'gender')[0];
      genderCount(queryAfter, female, function(err, genFemale){
        var femaleResultTwo = lodash.map(genFemale, 'gender')[0];
      genderCount(queryAfter, unknown, function(err, genUnknown){
        var unknownResultTwo = lodash.map(genUnknown, 'gender')[0];

      var changeMale = (maleResultTwo - maleResultOne)/maleResultOne;
      var changeFemale = (femaleResultTwo - femaleResultOne)/femaleResultOne;
      var changeUnknown = (unknownResultTwo - unknownResultOne)/unknownResultOne;

      resultsGender.push(changeMale, changeFemale, changeUnknown);
      callback(null, resultsGender);
                    });
                  });
                });
              });
            });
          });
    };

//set array for parameters
var opts = {
  locID: 8,
  placeID: 47,
  startDate: new Date(2016,0,1),
  endDate: new Date(2016,12,10),
  startDateTwo: new Date(2016,12,10),
  endDateTwo: new Date(2018,0,1),
};

var queryBefore = lodash.merge({}, opts, {
  startDate: opts.startDate,
  endDate: opts.endDate,
});

var queryAfter = lodash.merge({}, opts, {
  startDate: opts.startDateTwo,
  endDate: opts.endDateTwo,
});

connection.connect(function(_db) {
  db = _db;

  _init();
});

//set array for gender options
var optsArray = ['male', 'female', 'unknown'];

function _init() {

  //call genderAgeCount function with parameters
  optsArray.forEach(function(gender){
    genderAgeCount(opts, gender, console.log);
  });

  //call genderCount function with parameters
  optsArray.forEach(function(gender){
    genderCount(opts, gender, console.log);
  });

  //this takes no gender parameters as it counts only age groups
  ageCount(opts, function(err, ageResults) {
    console.log(ageResults);
  });

  //this function returns the percent change in age between two time periods
  //the values in the array represent genz, millennials, genx, boom, silent respectively
  ageChange(queryBefore, queryAfter, function(err, ageChangeResults){
      console.log(ageChangeResults);
    });

//this function returns the percent change in age between two time periods
  //the values in the array represent male, female, and unknown respectively
  genderChange(queryBefore, queryAfter, function(err, genderChangeResults) {
     console.log(genderChangeResults);
   });

};

