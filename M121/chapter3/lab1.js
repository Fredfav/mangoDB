// recall of the previous lab
// build the pipeline
var pipeline = [
    { $match : { 
	    "languages" : {$in: ["English"]},
	    "imdb.rating" : { $gte : 1 }, 
		"imdb.votes" : { $gte : 1 }, 
		"year" : { $gte : 1990 }
		} 
	},
	{ $addFields : 
	    { "scaled_votes" : 
	        { $add: [
                1,
                { $multiply: [
                    9,
                    { $divide: [
                        { $subtract: [ "$imdb.votes" , 5] },
                        { $subtract: [1521105, 5] }
                    ]}
                ]}
		    ]}
		}
	},
	{ $addFields : { "normalized_rating" : { $avg : [ "$scaled_votes", "$imdb.rating" ] } } },
	{ $sort : { "normalized_rating" : 1 } }
];

// print the result
var result = db.movies.aggregate(pipeline, { allowDiskUse : true }).next().title;
print("Result: " + result);

// lab 1 - group and accumulator
// build the pipeline
var pipeline = [
	{
		$match: { // all films that won at least 1 Oscar
			"awards": { $regex: /Won \d+ Oscar/ }
		}
	},
	{
		$group: { // calculate the standard deviation (sample operator), highest, lowest, and average imdb.rating
			_id: null,
			"highest_rating": { $max: "$imdb.rating" },
			"lowest_rating": { $min: "$imdb.rating" },
			"average_rating": { $avg: "$imdb.rating" },
			"deviation": { $stdDevSamp: "$imdb.rating"}
		}
	}
];

// print the result
print("Result lab 1: ");
printjson(db.movies.aggregate(pipeline).next());

//lab 2 - unwind
// build the pipeline What is the name, number of movies, and average rating (truncated to one decimal) for the cast member that has been in the most number of movies with English as an available language?
var pipeline = [
	{ $unwind: "$cast" },
	{
		$group: {
			"_id": "$cast",
			"numFilms": { $sum: 1 },
			"average": { $avg: "$imdb.rating" }
		}
	},
	{ $sort: { "numFilms": -1 } },
	{ $limit : 1 }
];

// print the result
print("Result lab 2: ");
printjson(db.movies.aggregate(pipeline).next());

//lab 3 - lookup
// Which alliance from air_alliances flies the most routes with either a Boeing 747 or an Airbus A380 (abbreviated 747 and 380 in air_routes)?
// build the pipeline
var pipeline = [
	{ $unwind: "$airlines"},
	{
		$lookup: {
			from: "air_routes",
			localField: "airlines",
			foreignField: "airline.name",
			as: "routes"
		}
	},
	{ $unwind: "$routes" },
	{ $match: { "routes.airplane": { $in: ["747", "380"] } } },
	{ $group: {
		"_id": "$name",
		"route_count": { $sum: 1 }
	}
	},
	{ $sort: { "route_count": -1 } }
];

// print the result
print("Result lab 3: ");
printjson(db.air_alliances.aggregate(pipeline).next());

//lab 4 - graphLookup
// Find the list of all possible distinct destinations, with at most one layover, departing from the base airports of airlines that make part of the "OneWorld" alliance. The airlines should be national carriers from
// Germany, Spain or Canada only. Include both the destination and which airline services that location. As a small hint, you should find 158 destinations.
print("Result lab 4:");
print("option 2:");
var pipeline = [{
  $match: { name: "OneWorld" }
}, {
  $graphLookup: {
    startWith: "$airlines",
    from: "air_airlines",
    connectFromField: "name",
    connectToField: "name",
    as: "airlines",
    maxDepth: 0,
    restrictSearchWithMatch: {
      country: { $in: ["Germany", "Spain", "Canada"] }
    }
  }
}, {
  $graphLookup: {
    startWith: "$airlines.base",
    from: "air_routes",
    connectFromField: "dst_airport",
    connectToField: "src_airport",
    as: "connections",
    maxDepth: 1
  }
}, {
  $project: {
    validAirlines: "$airlines.name",
    "connections.dst_airport": 1,
    "connections.airline.name": 1
  }
},
{ $unwind: "$connections" },
{
  $project: {
    isValid: { $in: ["$connections.airline.name", "$validAirlines"] },
    "connections.dst_airport": 1
  }
},
{ $match: { isValid: true } },
{ $group: { _id: "$connections.dst_airport" } }
];

//print the result
var result = db.air_alliances.aggregate(pipeline);
print("Result: ");
var i = 0;
while (result.hasNext()) {
    printjson(result.next());
	i = i + 1;
}
print(i);
