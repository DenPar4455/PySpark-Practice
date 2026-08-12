from pyspark.sql import SparkSession

import os

# set Python environments
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

# create spark session and context
spark = SparkSession.builder.appName("Local PySpark").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

# given RDD
rdd = sc.parallelize([("hat", 25), ("shirt", 40), ("hat", 30), ("shoes", 80), ("shirt", 20)])

# calculated total revenue per product saved in descending order
rdd2 = rdd.reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[0], False)

print(rdd.collect())  # show original
print(rdd2.collect()) # show revenue per product
print(rdd2.first())   # show highest grossing product