var favorites = [
  "Sandra Bullock",
  "Tom Hanks",
  "Julia Roberts",
  "Kevin Spacey",
  "George Clooney"];
  
// build the pipeline
var pipeline = [
	{$match: { // selection of the matching film (USA + rating >= 3 + casting)
		"tomatoes.viewer.rating":  {$gte: 3},
		"countries": {$in: ["USA"]},
		"cast": {$exists: true}
	}},
	{ // how many favorites appear in the cast field of the movie
		$addFields : { "num_favs" : { $size : { $setIntersection : [ "$cast", favorites ] } } } 
	},
	{ // Sort  results by num_favs, tomatoes.viewer.rating, and title, all in descending order.
		$sort: {
			"num_favs": -1,
			"tomatoes.viewer.rating": -1,
			"title": -1
		}
	},
	{ $skip : 24 } // show only the 25th film
];

// print the result
var result = db.movies.aggregate(pipeline, { allowDiskUse : true }).next().title;
print("Result: " + result);

var pipeline = [
    { $match : { 
	    "languages" : {$in: ["English"]}Ò,
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