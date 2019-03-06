import os, sys
import re
from pyspark import SparkConf, SparkContext

# configuration & initialization of the spark context
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile(os.path.join(os.environ['HOME'], "github/spark_class/data/book.txt"))

# define function
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

# -- MAP --
words = input.flatMap(normalizeWords)
# -- REDUCE --
wordCounts = words.countByValue()

# display results
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
