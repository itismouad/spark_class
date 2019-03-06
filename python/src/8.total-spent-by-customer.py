import os, sys
from pyspark import SparkConf, SparkContext

# configuration & initialization of the spark context
conf = SparkConf().setMaster("local").setAppName("TotalByCustomer")
sc = SparkContext(conf = conf)

lines = sc.textFile(os.path.join(os.environ['HOME'], "github/spark_class/data/customer-orders.csv"))

# define function
def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    itemID = int(fields[1])
    price = float(fields[2])
    return (customerID, itemID, price)

# -- MAP --
parsedLines = lines.map(parseLine)

# -- REDUCE --
# input type = (customerID, itemID, price)
# output type = (customerID, total spent)
custBaskets = parsedLines.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: x + y).sortByKey()

# collect and display results
results = custBaskets.collect();

for result in results:
    print(str(result[0]) + " : $" + str(round(result[1], 2)))
