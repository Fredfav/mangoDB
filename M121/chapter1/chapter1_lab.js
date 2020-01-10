//
// Lab 1
//
var pipeline = [
    { $match : 
	    { $and : [
		    { "imdb.rating" : { $gte : 7 }}, 
			{ "genres" :
			    { $nin :[ "Crime", "Horror" ]}
			},
			{ "rated" :  
				{ $in : [ "PG", "G" ]}
			},
			{ "languages" :
				{ $all : [ "English", "Japanese" ]}
			}
		]}
	}
];

// Check the pipeline (should output 23).
var count = db.movies.aggregate(pipeline).itcount();
print("Documents fetched by the pipeline (should be 23): " + count);

// Validates the solution.
load('validateLab1.js');
validateLab1(pipeline);

//
// Lab 2
//
var pipeline = [
    { $match : 
	    { $and : [
		    { "imdb.rating" : { $gte : 7 }}, 
			{ "genres" :
			    { $nin :[ "Crime", "Horror" ]}
			},
			{ "rated" :  
				{ $in : [ "PG", "G" ]}
			},
			{ "languages" :
				{ $all : [ "English", "Japanese" ]}
			}
		]}
	},
	{ $project : {
		"_id" : 0,
		"title" : 1,
		"rated" : 1
	}}
];

// Validates the solution
load('validateLab2.js');
validateLab2(pipeline);

//
// Lab 3
//
var pipeline = [
	{ $project : { 
		"nbwords" : { 
			$size : { $split : [ "$title" , " " ] }
			} 
		}
	},
    { $match : { "nbwords" : 1 } }
];

// Print the pipeline result
var count = db.movies.aggregate(pipeline).itcount();
print("Documents fetched by the pipeline (should be 8068): " + count);

//
// Optional Lab
//
// add the $count stage to the end of your pipeline
// you will learn about this stage shortly!
var pipeline = [
	{ $match: {
			"cast": { $elemMatch: { $exists: true } },
			"directors": { $elemMatch: { $exists: true } },
			"writers": { $elemMatch: { $exists: true } }
	}},
	{ $project : {
		"writers" : {
			$map : { 
				input: "$writers",
	            as: "writer",
	            in: { $arrayElemAt: [ { $split: [ "$$writer", " (" ] }, 0 ]}
			}
		},
		"cast" : 1,
		"directors" : 1 
	}},
	{ $project: { 
		"laborOfLove": { $gt: [ { $size: { $setIntersection: ["$writers", "$cast", "$directors"] } }, 0 ] } 
	}},
	{ $match : { "laborOfLove" : true } }
];

// Print the pipeline result
var count = db.movies.aggregate(pipeline).itcount();
print("Documents fetched by the pipeline (should be 1597): " + count);