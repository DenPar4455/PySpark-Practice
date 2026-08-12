from pyspark.sql import SparkSession

import os

# set Python environments
os.environ['PYSPARK_PYTHON'] = "/usr/bin/python3"
os.environ['PYSPARK_DRIVER_PYTHON'] = "/usr/bin/python3"

# create spark session and context
spark = SparkSession.builder.appName("DataFrame Manipulation").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# load CSV file as DataFrame
df = spark.read.csv("people.csv", header=True, inferSchema=True)

df.show()           # show original dataset
df.limit(3).show()  # show first few rows
df.printSchema()    # print Schema
print(df.count())   # count number of records

# new DataFrame with only poeple from New York
df2 = df.filter(df.city == "New York")

df2.select("name", "salary").show() # show only name and salary
print(df2.count())                  # show number of poeple in NY
df2.agg({"salary":"avg"}).show()    # show their average salary

# show the average of salary per city orderd in descending order
df.groupBy("city").agg(sf.avg("salary")).orderBy(["avg(salary)"], ascending=[False]).show()

# add a new column of the income bracket of each person
df3 = df.withColumn("income_bracket", sf.when(df["salary"] < 80000, "Low").otherwise(sf.when(df["salary"] >= 100000, "High").otherwise("Mid")))

# show number of people that fall into each income bracket
df3.groupBy("income_bracket").agg({"*":"count"}).show()
