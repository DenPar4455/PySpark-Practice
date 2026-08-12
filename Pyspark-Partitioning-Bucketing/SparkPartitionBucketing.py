from pyspark.sql import SparkSession

import os

# set Python environmentss
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

# create spark session and context
spark = SparkSession.builder.appName("Partitioning and Bucketing Test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# prepare data and columns
data = [
    (101, "Laptop", "Electronics", 1200, "2024-01-15"),
    (102, "Headphones", "Electronics", 200, "2024-01-17"),
    (103, "Coffee Maker", "Home", 85, "2024-01-20"),
    (104, "Desk", "Office", 300, "2024-02-01"),
    (105, "Monitor", "Electronics", 400, "2024-02-03"),
    (106, "Blender", "Home", 60, "2024-02-10"),
    (107, "Chair", "Office", 150, "2024-02-14"),
    (108, "Keyboard", "Electronics", 90, "2024-02-18"),
    (109, "Lamp", "Home", 40, "2024-02-20"),
    (110, "Notebook", "Office", 10, "2024-02-21")
]

columns = ["product_id", "product_name", "category", "price", "sale_date"]

# create dataframe with data and columns
df = spark.createDataFrame(data, columns)

# show original data
df.show()

# Write DataFrame as a partitioned Parquet file based on the category column
df.write.mode("overwrite").partitionBy("category").parquet("products_partitioned")

# Write data as a table bucketed by the price column into 5 buckets
df.write.bucketBy(5, "price").mode("overwrite").sortBy("price").saveAsTable("products_bucketed")

# show table through query
spark.sql("SELECT * FROM products_bucketed;").show()

# run query to filter electronic products with price greater than 100 on both 
spark.read.parquet("products_partitioned").select('*').filter("category = 'Electronics' AND price > 100").show()
spark.sql("SELECT * FROM products_bucketed WHERE category = 'Electronics' AND price > 100").show()