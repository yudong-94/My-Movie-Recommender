# My Movie Recommender
This is my Recommender System project based on movie ratings.

## Data Source
All the data I used come from the [MovieLens dataset](https://grouplens.org/datasets/movielens/).  
Specifically, I used the information of movie id, user id, movie ratings, and movie genres.

## Description
I built a hybrid recommender system in this case. It contains the following 4 small recommenders:  
1. User-based Collaborative Filtering Recommender
This is the main part of the recommender. I used the user-based CF algorithm - compute user similarity, and predict the user-movie ratings based on the user's similar users and the ratings they given on the movie.  
2. User-Genre Average Recommonder  
This one predict the user-movie rating as the average rating of the user of the movie from the same genre. When there is multiple genres the movie belongs to, the prediction would be a weighted average rating based on the number of movies the user has rated in each genre.  
3. User Average Recommender  
It simply predicts the user-movie rating as the average historical rating of the user.  
4. Movie Average Recommender  
It simply predicts the user-movie rating as the average historical rating given to the movie.  

Based on the 4 recommenders, I simply take the average of the predicted ratings as the final prediction.

## Command to Run
userName$ ./bin/spark-submit --class rec comp.jar ratings.csv testing_small.csv movies.csv  
Spark version: 2.10.6

## Accuracy
>=0 and <1: 1  
>=1 and <2: 54  
>=2 and <3: 2318  
>=3 and <4: 14766  
>=4: 3117  
RMSE = 0.9145703756805906  
The total execution time taken is 74.41591609 sec.  

## Next Step
1. Tune the weight of each small recommenders by Cross-validation to get higher accuracy.  
2. Introduce more external data such as user genders, movie names, IMDB information (director, actor, average rating, ...).  


