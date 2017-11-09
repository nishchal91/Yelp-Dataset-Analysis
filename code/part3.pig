businessJsonLoad = LOAD 'yelp_academic_dataset_business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (business:map []);

generateCustomData = 
	FOREACH businessJsonLoad 
		GENERATE 
        	(float)business#'latitude' as latitude,
            (float)business#'longitude' as longitude,
            (float)business#'stars' as starRating,
            Flatten(business#'categories') as categories;


businessNearUniversityOfWisconsin= 
	FILTER generateCustomData BY 
		latitude>42.9083 and latitude <43.2416 and
    	longitude>-89.5839 and longitude<-89.2505;
                
groupedDataByCategories= GROUP businessNearUniversityOfWisconsin by categories;

averageRatingOfBusiness= 
	FOREACH groupedDataByCategories
		GENERATE
           	group as category,
            AVG(businessNearUniversityOfWisconsin.starRating) as averageRank;

orderedDataByRank =ORDER averageRatingOfBusiness by category;
                    
Store orderedDataByRank into 'nn1123/part3_3' using PigStorage(',');