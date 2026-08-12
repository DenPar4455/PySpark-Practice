from pyspark.sql import SparkSession
from pyspark import StorageLevel

import os

# set up pySpark environments
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

# create a spark Session
spark = SparkSession.builder.appName("Local PySpark").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# create a Data Frame
df = spark.createDataFrame([{"id": 1, "name": "Alice", "age": 28},
  {"id": 2, "name": "Bob", "age": 33},
  {"id": 3, "name": "Cathy", "age": 45},
  {"id": 4, "name": "David", "age": 23},
  {"id": 5, "name": "Eva", "age": 31}])

df.cache()  # cache the DataFrame to memory only
print(df.collect())  # run two collect actions
print(df.collect())

df.persist(StorageLevel.MEMORY_AND_DISK) # persist the DataFrame to memory and disk
print(df.collect())  # run two collect actions
print(df.collect())