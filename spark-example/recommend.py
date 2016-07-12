
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

    print '\n'
    #print colored(userA['user'], 'yellow')
    #print colored( userAprofile, 'blue')
    #print colored(userB['user'], 'yellow')
    #print colored( userBprofile, 'blue')

    moviesRatedByA = set(userAprofile.keys())
    moviesRatedByB = set(userBprofile.keys())
    moviesBothUsersRated = moviesRatedByA.intersection( moviesRatedByB )
    moviesOnlyBRated = moviesRatedByB.difference( moviesRatedByA )

    if len(moviesBothUsersRated) < 2:
        return None
    #print colored(moviesBothUsersRated, 'yellow')

    similarity = 0.0
    seriesA = []
    seriesB = []
    for movie in moviesBothUsersRated:
        #print userA['user'], 'rated', movie, userAprofile[movie]
        #print userB['user'], 'rated', movie, userBprofile[movie]
        seriesA.append( userAprofile[movie] )
        seriesB.append( userBprofile[movie] )
        similarity += userAprofile[movie] * userBprofile[movie]

    #similarity = pearsonr(seriesA, seriesB)
    #similarity = similarity[0] * len(moviesBothUsersRated)
    #print similarity / (len(moviesBothUsersRated) * 25)

    return {
        'user' : userA['user'],
        'other': userB['user'],
        'similarity' : similarity,
        'recommendations': list(moviesOnlyBRated)
    }

def findKNN(userSimilarities):

    k = 5

    for sim in userSimilarities:
        print sim

    return userSimilarities

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
        .groupBy(lambda rating: rating['user']) \
        .map(buildProfileFromGroup) \
        .cache()

    # compute similarity between users' profiles
    similarityGraph = userProfiles.cartesian(userProfiles) \
        .take(10000)
        # TODO cut here
    similarityGraph = sc.parallelize(similarityGraph) \
        .map(computeSimilarity) \
        .filter(lambda similarity: similarity != None) \
        .groupBy(lambda similarity: similarity['user']) \
        .take(100)

    finalRecommendations = {}
    for sim in similarityGraph:

        userId = sim[0]
        recommendations = sim[1]
        print colored(sim, 'yellow')

        userRec = sc.parallelize(recommendations) \
            .sortBy(lambda rec: rec['similarity'], ascending=False) \
            .take(10)

        for uRec in userRec:

            recsFromUser = set(uRec['recommendations'])

            if not userId in finalRecommendations:
                finalRecommendations[userId] = recsFromUser
            else:
                unionSet = finalRecommendations[userId].intersection( recsFromUser )
                if len(unionSet) > 5:
                    finalRecommendations[userId] = unionSet

        finalRecommendations[userId] = list(finalRecommendations[userId])

    print colored(finalRecommendations, 'green')
    with open(workingDir + 'recommendations.json', 'w') as outfile:
        json.dump(finalRecommendations, outfile, indent=4)
