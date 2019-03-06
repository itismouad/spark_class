import os, sys
import codecs
from pyspark import SparkConf, SparkContext

# configuration & initialization of the spark context
conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

# define filenames
ratings_filename = os.path.join(os.environ['HOME'], "github/spark_class/data/ml-100k/u.data")
movie_filename = os.path.join(os.environ['HOME'], "github/spark_class/data/ml-100k/u.ITEM")

lines = sc.textFile(ratings_filename)

# define function(s)
def parseLine(line):
    fields = line.split()
    userID = int(fields[0])
    movieID = int(fields[1])
    Rating = float(fields[2])
    Timestamp = float(fields[3])
    return (userID, movieID, Rating, Timestamp)

def loadMovieNames(movie_filename):
    movieNames = {}
    # The following line is updated from what's in the video to handle
    # encoding issues on some Ubuntu systems:
    with codecs.open(movie_filename, "r", encoding='utf-8', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# -- BROAADCAST --
# brodcast to every cluster so it is available when needed
nameDict = sc.broadcast(loadMovieNames(movie_filename))

# -- MAP --
parsedLines = lines.map(parseLine)

# -- REDUCE --
# input type = (UserID, MovieID, Rating, Timestamp)
# output type = (customerID, total spent)
movieCounts = parsedLines.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y)
flipped = movieCounts.map(lambda x: (x[1], x[0])).sortByKey()

sortedMoviesWithNames = flipped.map(lambda item : (nameDict.value[item[1]], item[0]))

# collect and display results
results = sortedMoviesWithNames.collect();

for result in results:
    print(result)
