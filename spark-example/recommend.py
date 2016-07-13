
from pyspark import SparkContext, SparkConf
import numpy as np
from scipy.stats.stats import pearsonr
from termcolor import colored
import json

def buildRatingFromLine(l):

    fields = l.split(',')

    return {
        'user'  : fields[0],
        'movie' : fields[1],
        'rating': float(fields[2])
    }

def buildProfileFromGroup(g):

    profile = {}
    for rating in g[1]:
        movieId = rating['movie']
        profile[movieId] = rating['rating']

    return {
        'user'   : g[0],
        'profile': profile
    }

def computeSimilarity(twoUsers):

    userA = twoUsers[0]
    userB = twoUsers[1]

    # don't compare a user with himself
    if(userA['user'] == userB['user']):
        return None

    userAprofile = userA['profile']
    userBprofile = userB['profile']

    moviesRatedByA = set(userAprofile.keys())
    moviesRatedByB = set(userBprofile.keys())
    moviesBothUsersRated = moviesRatedByA.intersection( moviesRatedByB )
    moviesRated = moviesRatedByA.union( moviesRatedByB )
    moviesOnlyBRated = moviesRatedByB.difference( moviesRatedByA )

    similarity = float( len(moviesBothUsersRated) ) / float( len(moviesRated) )

    return {
        'user' : userA['user'],
        'other': userB['user'],
        'similarity' : similarity,
        'recommendations': list(moviesOnlyBRated)
    }

def getRecommendationsFromKNN(kNearestNeighBours):

    recommendations = []
    for i, neighbour in enumerate(kNearestNeighBours):

        recsFromUser = set(neighbour['recommendations'])

        if i == 0:
            recommendations = recsFromUser
        else:
            unionSet = recommendations.intersection( recsFromUser )
            if len(unionSet) > 1:
                recommendations = unionSet

        return list( recommendations )

if __name__ == '__main__':

    # configure cluster
    conf = SparkConf()
    conf.set("spark.master", "local[*]")

    # get context
    sc = SparkContext(conf=conf)

    # working directory
    workingDir = "/home/piero/Desktop/progetti/corsi/machine_learning/machine-learning-open-course/spark-example/data/"

    # load file and build user profiles
    fileName = workingDir + "ratings.csv"
    userProfiles = sc.textFile(fileName) \
        .filter(lambda line: not 'userId' in line) \
        .map(buildRatingFromLine) \
        .groupBy(lambda profile: profile['user']) \
        .map(buildProfileFromGroup) \
        .cache()

    # keep a list of user IDs for later
    userIds = userProfiles.map(lambda profile: profile['user']) \
        .collect()

    # compute similarity between users' profiles
    similarityGraph = userProfiles.cartesian(userProfiles) \
        .map(computeSimilarity) \
        .filter(lambda similarity: similarity != None) \
        .filter(lambda similarity: similarity['similarity'] > 0.03) \
        .cache()

    # take recommendations from the most similar users
    finalRecommendations = {}
    for i, userId in enumerate(userIds):

        print colored(str(i) + ': ' + userId, 'yellow')

        kNearestNeighBours = similarityGraph.filter(lambda rec: rec['user'] == userId) \
            .sortBy(lambda rec: rec['similarity'], ascending=False) \
            .take(10)

        print kNearestNeighBours

        finalRecommendations[userId] = getRecommendationsFromKNN( kNearestNeighBours )

        #break

    # save to file
    print colored(finalRecommendations, 'green')
    with open(workingDir + 'recommendations.json', 'w') as outfile:
        json.dump(finalRecommendations, outfile, indent=4)
