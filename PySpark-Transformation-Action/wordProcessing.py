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
rdd = sc.parallelize(["love this product", "not worth the price", "highly recommend", "do not buy", "amazing quality", "very bad experience"])

# split phrases into words
rdd2 = rdd.flatMap(lambda phrase: phrase.split(" "))

# count the frequency of each word
rdd3 = rdd2.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

# filter only positive words
rdd4 = rdd3.filter(lambda word: word[0] if word[0] in ['love', 'recommend', 'amazing', 'quality', 'highly'] else None)

print(rdd.collect())    # show original
print(rdd2.count())     # show word count
print(rdd3.collect())   # show frequency of all words
print(rdd4.collect())   # show frequency of positive words
                          