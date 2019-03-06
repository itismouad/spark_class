import os, sys
from pyspark import SparkConf, SparkContext

# configuration & initialization of the spark context
conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

lines = sc.textFile(os.path.join(os.environ['HOME'], "github/spark_class/data/ml-100k/u.data"))

# define function
def parseLine(line):
    fields = line.split()
    userID = int(fields[0])
    movieID = int(fields[1])
    Rating = float(fields[2])
    Timestamp = float(fields[3])
    return (userID, movieID, Rating, Timestamp)

# -- MAP --
parsedLines = lines.map(parseLine)

# -- REDUCE --
# input type = (UserID, MovieID, Rating, Timestamp)
# output type = (customerID, total spent)
movieCounts = parsedLines.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y)
flipped = movieCounts.map(lambda x: (x[1], x[0])).sortByKey()

# collect and display results
results = flipped.collect();

for result in results:
    print(result)
