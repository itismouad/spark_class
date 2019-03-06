import os, sys
from pyspark import SparkConf, SparkContext

# configuration & initialization of the spark context
conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)

# define function
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile(os.path.join(os.environ['HOME'], "github/spark_class/data/1800.csv"))
parsedLines = lines.map(parseLine)

# get min temprature in dataset for each stationID
# input type = (stationID, entryType, temperature)
# output type = (stationID, min temperature)
# -- MAP --
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# -- REDUCE --
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

# collect and display results
results = minTemps.collect();
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
