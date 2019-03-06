import os, sys
import codecs
from pyspark import SparkConf, SparkContext

# configuration & initialization of the spark context
conf = SparkConf().setMaster("local").setAppName("PopularHero")
sc = SparkContext(conf = conf)

# define filenames
names_filename = os.path.join(os.environ['HOME'], "github/spark_class/data/marvel-names.txt")
graph_filename = os.path.join(os.environ['HOME'], "github/spark_class/data/marvel-graph.txt")

names_lines = sc.textFile(names_filename)
graph_lines = sc.textFile(graph_filename)

# define function(s)
def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements) - 1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode("utf8"))


# process data
# -- MAP --
pairings = graph_lines.map(countCoOccurences)
names = names_lines.map(parseNames)
# -- REDUCE --
# input type = (character ID, count of co-occurrences)
totalFriendsByCharacter = pairings.reduceByKey(lambda x, y : x + y)
flipped = totalFriendsByCharacter.map(lambda item : (item[1], item[0]))

# get most popular
mostPopular = flipped.max()
mostPopularName = names.lookup(mostPopular[1])[0]

# display results
print(str(mostPopularName) + " is the most popular superhero, with " + \
    str(mostPopular[0]) + " co-appearances.")
