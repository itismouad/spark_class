import os, sys
import collections
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession that will give us both a Spark context and a SQL context
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
sc = spark.sparkContext

filename = os.path.join(os.environ['HOME'], "github/spark_class/data/fakefriends.csv")

# define function(s)
def mapper(line):
    '''
    Take an RDD value (=line) and converts it to a DataFrame Row
    '''
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

# Reading as an RDD (`line`) then transforming it as a DataFrame `people`
# by converting every line in a Row of 4 fields (ID, name, age, numFriends)
lines = sc.textFile(filename)
people = lines.map(mapper)

# Infer the schema
# Register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)

# We can also use functions (mirrored on classic pandas functions) instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
