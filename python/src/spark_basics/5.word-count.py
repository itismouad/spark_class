import os, sys
from pyspark import SparkConf, SparkContext

# configuration & initialization of the spark context
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile(os.path.join(os.environ['HOME'], "github/spark_class/data/book.txt"))

# -- MAP --
words = input.flatMap(lambda x: x.split())
# -- REDUCE --
wordCounts = words.countByValue()

# display results
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
