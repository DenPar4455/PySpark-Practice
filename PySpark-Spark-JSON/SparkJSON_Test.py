from pyspark.sql import SparkSession

import os

# set Python environmentss
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

# create spark session and context
spark = SparkSession.builder.appName("SparkJSON Test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load JSON file as Data Frame
df = spark.read.option("multiline","true").json("employees.json")

df.printSchema()        # Display the schema
df.show(truncate=False) # Show the data

# assign default salary to missing values
df_fill = df.na.fill({"salary": 40000})
df_fill.show()

# wirte DataFrame to employees_cleaned.json
df_fill.write.mode("overwrite").json("employees_cleaned.json")

spark.stop()
