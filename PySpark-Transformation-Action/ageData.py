from pyspark.sql import SparkSession

import os

# set Python environments
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

# create spark session and context
spark = SparkSession.builder.appName("Local PySpark").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

# Create an RDD from list of ages
dataRDD = [15, 22, 35, 42, 60, 18, 27, 19, 75, 29]
rdd = sc.parallelize(dataRDD)

# label ages with 'minor', 'adult', or 'senior'
rdd2 = rdd.map(lambda age: (age, "minor") if age < 18 else (age, "senior") if age > 64 else (age, "adult"))

# keep only labels
rdd3 = rdd2.map(lambda x: x[1])

# filter to kep only adults
rdd4 = rdd.filter(lambda age: age >= 18 and age <= 64)

print(rdd.collect())    # show original
print(rdd2.collect())   # show ages with labels
print(rdd3.countByValue().items())  # show category counts 
print(rdd4.collect())   # show adult ages from original dataset