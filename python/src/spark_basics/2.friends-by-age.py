import os, sys
from pyspark import SparkConf, SparkContext

# configuration & initialization of the spark context
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

# define function
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile(os.path.join(os.environ['HOME'], "github/spark_class/data/fakefriends.csv"))
rdd = lines.map(parseLine)

# count up sum of friends and # of entries per age
# input type (age, # of friends)
# output type (age, (sum of friends, sum of persons that age))
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# input type (age, (sum of friends, sum of persons that age))
# output (age, average # of friends)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# collect and display results
results = averagesByAge.collect()
for result in results:
    print(result)
