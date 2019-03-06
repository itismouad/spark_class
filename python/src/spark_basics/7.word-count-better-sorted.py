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
# we could use `words.countByValue()` and sort by let us RDDs to keep it scalable

# input type = (list of words)
# output type = (word, count of occurence)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

# input type = (word, count of occurence)
# output type = (count of occurence, word)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

# display results
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
