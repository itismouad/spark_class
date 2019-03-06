import os, sys
from pyspark.sql import SparkSession, Row, functions

# Create a SparkSession that will give us both a Spark context and a SQL context
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
sc = spark.sparkContext

# define filenames
ratings_filename = os.path.join(os.environ['HOME'], "github/spark_class/data/ml-100k/u.data")
movie_filename = os.path.join(os.environ['HOME'], "github/spark_class/data/ml-100k/u.ITEM")

# define function(s)
def loadMovieNames(movie_filename):
    movieNames = {}
    with open(movie_filename, "r", encoding='ascii', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Load data files into an RDD and a dictionar
lines = sc.textFile(ratings_filename)
nameDict = loadMovieNames(movie_filename)

# Convert it to a RDD of Row objects, then a DataFrame
movies = lines.map(lambda x: Row(movieID =int(x.split()[1])))
movieDataset = spark.createDataFrame(movies)

topMovieIDs = (movieDataset
                .groupBy("movieID")
                .count()
                .orderBy("count", ascending=False)
                .cache()
                )

topMovieIDs.show()

# Grab the top 10
top10 = topMovieIDs.take(10)

# Print the results
print("\n")
for result in top10:
    # Each row has movieID, count as above.
    print("%s: %d" % (nameDict[result[0]], result[1]))

# Stop the session
spark.stop()
