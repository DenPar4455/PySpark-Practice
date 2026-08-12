from pyspark.sql import SparkSession

import os

# set Python environmentss
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

# create spark session and context
spark = SparkSession.builder.appName("SparkSQL Test").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load CSV file as Data Frame
df = spark.read.csv("orders.csv", header=True, inferSchema=True)

# Register DataFrame as a temporary view called orders
df.createOrReplaceTempView("orders")

# show all records from the view
spark.sql("SELECT * FROM orders").show()

# show only orders with 'shipped' status
spark.sql("SELECT * FROM orders WHERE status = 'shipped'").show()

# show total order amount of each customer
spark.sql("SELECT customer, SUM(amount) AS total_order_amount FROM orders GROUP BY customer").show()

# show orders of amounts bigger than 100
spark.sql("SELECT * FROM orders WHERE amount > 100 ORDER BY amount DESC").show()

# show customers who have more than one order
spark.sql("SELECT customer, COUNT(*) AS total_orders FROM orders GROUP BY customer HAVING total_orders > 1").show()

# show top 3 customers by total order amount in descending order
spark.sql("SELECT customer, SUM(amount) AS total_order_amount FROM orders GROUP BY customer ORDER BY total_order_amount DESC LIMIT 3").show()