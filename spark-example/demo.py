import pandas as pd
import json
from pprint import pprint

# load recommendation data
with open('data/recommendations.json') as json_data:
    recommendations = json.load(json_data)

# load user ratings and movies' names
ratings = pd.read_csv('data/ratings.csv')
movies = pd.read_csv('data/movies.csv')
ratings = pd.merge(ratings, movies, on='movieId')

while(True):

    # ask for user. Good examples are 2, 14, 35, 51 ...
    user = raw_input('User ID? ')
    print '============='

    print '\nUser', user, ' already watched:'
    userRatings = ratings[ ratings['userId'] == int(user) ]
    print userRatings['title'].values

    print '\nRecommended'
    userRecommendations = [int(r) for r in recommendations[user]]
    userRecommendations = movies[ movies['movieId'].isin(userRecommendations)]
    print userRecommendations['title'].values
    print '\n\n\n============='