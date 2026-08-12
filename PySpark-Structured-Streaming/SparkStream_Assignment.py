from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import os

# set Python environmentss
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

# create spark session and context
spark = SparkSession.builder.appName("SparkStream Test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load CSV file as Data Frame
df = spark.read.csv("orders.csv", header=True, inferSchema=True)

# define schema to match JSON structure
json_schema = StructType([
    StructField("device_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperature", DoubleType()),
    ])

# create DataFrame from batch1.json
stream_df = spark.readStream.format("json").schema(json_schema).load("/opt/spark/work-dir/temp_input/")

# select devices with temperature above 70
result_df = stream_df.select("device_id", "temperature").filter(stream_df["temperature"] > 70)

# write query in console
query = result_df.writeStream.format("console").outputMode("append").start()

try:
    # watch the output
    query.awaitTermination()
except KeyboardInterrupt:
    # stop the stream
    query.stop()