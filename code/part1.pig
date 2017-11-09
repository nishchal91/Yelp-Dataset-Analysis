businessJsonLoad = LOAD 'yelp_academic_dataset_business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (business:map []);

generateCustomData = 
	FOREACH businessJsonLoad 
		GENERATE 
        	business#'name' as name,
            (float)business#'latitude' as latitude,
            (float)business#'longitude' as longitude,
            business#'state' as state,
            business#'city' as city,
            (int)business#'review_count' as numberOfReviews,
            Flatten(business#'categories') as categories;


getUSCities= FILTER generateCustomData BY 
				latitude>26.24 and latitude <49.52 and
                longitude>-124.65 and longitude<-66.75 and
                city!='Montréal' and city!='Ajax' and
                city!='Toronto' and state!='QC' and state!='ON';
                
groupedDataByCityAndCategories= GROUP getUSCities by (city,categories);

finalOutput= FOREACH groupedDataByCityAndCategories
				GENERATE
                	group.city,
                    group.categories,
                    SUM(getUSCities.numberOfReviews);
                    
Store finalOutput into 'nn1123/part1_6' using PigStorage(',');