import os, sys
import collections
from pyspark import SparkConf, SparkContext

# configuration & initialization of the spark context
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# LOAD : breaks line by line to create the values of the rdd
# MAP (RDD >> RDD) : split them and take the 3rd column
# REDUCE : now that we have values, perform a `values_count` operation to build a histogram
lines = sc.textFile(os.path.join(os.environ['HOME'], "github/spark_class/data/ml-100k/u.data"))
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

# sort our results
sortedResults = collections.OrderedDict(sorted(result.items()))

# print our results
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
