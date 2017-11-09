businessJsonLoad = LOAD 'yelp_academic_dataset_business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (business:map []);
reviewJsonLoad = LOAD 'yelp_academic_dataset_review.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (review:map []);


generateBusinessData = 
	FOREACH businessJsonLoad 
		GENERATE
        	business#'business_id' as businessID,
            business#'name' as businessName,
        	(float)business#'latitude' as latitude,
            (float)business#'longitude' as longitude,
            (float)business#'stars' as starRating,
            FLATTEN(business#'categories') as categories;


businessNearUniversityOfWisconsin= 
	FILTER generateBusinessData BY 
		latitude>42.9083 and latitude <43.2416 and
    	longitude>-89.5839 and longitude<-89.2505;
        
foodBusinessNearUniversityOfWisconsin= Filter businessNearUniversityOfWisconsin BY (categories=='Food');

orderByStarsDesc= Order foodBusinessNearUniversityOfWisconsin By starRating DESC;
topTenFoodBusiness= LIMIT orderByStarsDesc 10;


orderByStarsAsc= Order foodBusinessNearUniversityOfWisconsin By starRating ASC;
bottomTenFoodBusiness= LIMIT orderByStarsAsc 10;

                
generateReviewData = 
	FOREACH reviewJsonLoad 
		GENERATE
        	review#'business_id' as businessID,
        	(float)review#'stars' as starRating,
            (datetime)review#'date' as date;
            
reviewsInJanToMay= FILTER generateReviewData BY
						GetMonth(date)>=1 and GetMonth(date)<=5;						

joinedTopTenData= JOIN topTenFoodBusiness by businessID,reviewsInJanToMay by businessID;

customViewForTopTen= FOREACH joinedTopTenData
				GENERATE
                	topTenFoodBusiness::businessName as businessName,
                    topTenFoodBusiness::businessID as businessID,
                    reviewsInJanToMay::starRating as rating,
                    GetMonth(reviewsInJanToMay::date) as month;
                    
groupCustomViewForTopTen= GROUP customViewForTopTen by (businessID,businessName,month);

flattenGroupCustomViewForTopTen= FOREACH groupCustomViewForTopTen 
							GENERATE 
                            	FLATTEN(group) as (businessID,businessName,month),
                                AVG(customViewForTopTen.rating);


joinedBottomTenData= JOIN bottomTenFoodBusiness by businessID,reviewsInJanToMay by businessID;

customViewForBottomTen= FOREACH joinedBottomTenData
				GENERATE
                	bottomTenFoodBusiness::businessName as businessName,
                    bottomTenFoodBusiness::businessID as businessID,
                    reviewsInJanToMay::starRating as rating,
                    GetMonth(reviewsInJanToMay::date) as month;
                    
groupCustomViewForBottomTen= GROUP customViewForBottomTen by (businessID,businessName,month);

flattenGroupCustomViewForBottomTen= FOREACH groupCustomViewForBottomTen 
							GENERATE 
                            	FLATTEN(group) as (businessID,businessName,month),
                                AVG(customViewForBottomTen.rating);             


Store flattenGroupCustomViewForTopTen into 'nn1123/part5_12' using PigStorage(',');
Store flattenGroupCustomViewForBottomTen into 'nn1123/part5_13' using PigStorage(',');
