SET elephantbird.jsonloader.nestedLoad 'true';

businessJsonLoad = LOAD '/user/nn1123/yelp_academic_dataset_business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (business:map []);

generateCustomData = 
	FOREACH businessJsonLoad 
		GENERATE 
        	business#'name' as name,
            business#'city' as city,
            (int)business#'review_count' as numberOfReviews,
            (float)business#'stars' as starRating,
            Flatten(business#'categories') as categories;

                
groupedDataByCityAndCategories= GROUP generateCustomData by (city,categories);

averageRatingsForEachCategory=
	FOREACH groupedDataByCityAndCategories
    	GENERATE
        	group.categories as category,
            group.city as city,
            AVG(generateCustomData.starRating) as rating;


ranking= ORDER averageRatingsForEachCategory by category DESC , rating DESC;

                    
Store ranking into 'nn1123/part2_4';