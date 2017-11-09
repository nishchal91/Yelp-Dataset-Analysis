reviewJsonLoad = LOAD 'yelp_academic_dataset_review.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (review:map []);

businessJsonLoad = LOAD 'yelp_academic_dataset_business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (business:map []);

userJsonLoad = LOAD 'yelp_academic_dataset_user.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (user:map []);

generateReviewData = 
	FOREACH reviewJsonLoad 
		GENERATE 
        	review#'user_id' as userID,
            (float)review#'stars' as starRating,
            review#'business_id' as businessID;


generateUserData=
	FOREACH userJsonLoad
    	GENERATE
        	user#'user_id' as user_id,
            user#'name' as userName,
            (int)user#'review_count' as numberOfReviews;
            
orderGeneratedUserdataByReviewCount= ORDER generateUserData by numberOfReviews DESC;

getTopTen= LIMIT orderGeneratedUserdataByReviewCount 10;



joinUserAndReviews= JOIN getTopTen by user_id, generateReviewData by userID;

userAndReviewData=
	FOREACH joinUserAndReviews
    	GENERATE
        	userName as userName,
        	starRating as starRating,
            businessID as businessID;
        	

generateBusinessData=
	FOREACH businessJsonLoad
    	GENERATE
        	business#'business_id' as businessID,
            business#'categories' as category;
            
joinAll= JOIN generateBusinessData BY businessID , userAndReviewData by businessID;


flattenedGeneratedJoinedData= FOREACH joinAll
								GENERATE	
                                	userName,
                                	starRating,
                                    FLATTEN(category);
                                    
groupedDataByReviewers= GROUP flattenedGeneratedJoinedData by (userName,category);


ouputData=
	FOREACH groupedDataByReviewers
    	GENERATE
        	FLATTEN(group) as (userName,category),
            AVG(flattenedGeneratedJoinedData.starRating)as starRating;
            

Store getTopTen into 'nn1123/part4_a' using PigStorage(',');
Store ouputData into 'nn1123/part4_b' using PigStorage(',');
