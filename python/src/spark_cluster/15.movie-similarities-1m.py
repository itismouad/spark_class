import os, sys
from math import sqrt
from pyspark import SparkConf, SparkContext

#To run on EMR successfully + output results for Star Wars:
#aws s3 cp s3://sundog-spark/MovieSimilarities1M.py ./
#aws s3 sp c3://sundog-spark/ml-1m/movies.dat ./
#spark-submit --executor-memory 1g MovieSimilarities1M.py 260

# define filenames and data
ratings_filename = "ml-1m/ratings.dat"
movie_filename = "movies.dat"

# define function(s)
def loadMovieNames(movie_filename):
    movieNames = {}
    with open(movie_filename, "r", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def makePairs( userRatings ):
    '''
    reformat tuples
    '''
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return ((movie1, movie2), (rating1, rating2))

def filterDuplicates( userRatings ):
    '''
    filter duplicates by only having tuples in the format movieID_1 < movieID_2
    '''
    ratings = userRatings[1]
    (movie1, rating1) = ratings[0]
    (movie2, rating2) = ratings[1]
    return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
    '''
    Compute cosine similiarities
    ratingPairs = ((movie1, movie2), (rating1, rating2))
    result = ((movie1, movie2), (score, numPairs))
    '''
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))

    return (score, numPairs)

# Loading data
ratings_data = sc.textFile(ratings_filename)
names_data = loadMovieNames(movie_filename)

# -- MAP --
# ratings to key / value pairs
# input type = RDD line
# output type = (userID, (movieID, rating))
ratings = (ratings_data
                .map(lambda l: l.split())
                .map(lambda l: (int(l[0]), (int(l[1]), float(l[2]))))
                )

# -- JOIN --
# We use self-`join` to find every combination of userID
# input type = (userID, (movieID, rating))
# output type = (userID, ((movie1, rating1), (movie2, rating2))
joinedRatings = ratings.join(ratings)


# -- FILTER DUPLICATES --
# Filter out duplicate pairs
uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

# Now key by (movie1, movie2) pairs.
# input type = (userID, ((movie1, rating1), (movie2, rating2))
# output type = ((movie1, movie2), (rating1, rating2))
moviePairs = uniqueJoinedRatings.map(makePairs)


# Now collect all ratings for each movie pair and compute similarity
# input type = ((movie1, movie2), (rating1, rating2))
# output type = ((movie1, movie2), (score, numPairs))
moviePairRatings = moviePairs.groupByKey()
moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

# Save the results if desired
# moviePairSimilarities.sortByKey()
# moviePairSimilarities.saveAsTextFile("movie-sims")

# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 50

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda pairSim: \
        (pairSim[0][0] == movieID or pairSim[0][1] == movieID) \
        and pairSim[1][0] > scoreThreshold and pairSim[1][1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda pairSim: (pairSim[1], pairSim[0])).sortByKey(ascending = False).take(10)

    print("Top 10 similar movies for " + names_data[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(names_data[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
