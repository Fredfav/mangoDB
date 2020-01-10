// Build the pipeline
var pipeline = [
	{ // selection of films where English is an available language, the minimum imdb.rating is at least 1, the minimum imdb.votes is at least 1, and it was released in 1990 or after
		$match: {
			"imdb.rating": { $gte: 1 },
			"imdb.votes": { $gte: 1 },
			"year": { $gte: 1 },
			"languages": { $in: ["English"] }
		}
	},
	{ // compute and add scaled_votes
		$addFields: {
			"scaled_votes": 
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
	{ // compute normalized_rating
		$addFields : { "normalized_rating" : { $avg : [ "$scaled_votes", "$imdb.rating" ] } }
	},
	{ // sort by desc
		{ $sort : { "normalized_rating" : 1 } }
	}
];

// Prints the result.
var result = db.movies.aggregate(pipeline, { allowDiskUse : true }).next().title;
print("Result: " + result);