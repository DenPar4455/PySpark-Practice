from pyspark.sql import SparkSession
# Removed because it interfered with Python's built in round() function
# from pyspark.sql.functions import * 
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
#import time to measuere elapsed time
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SharedVariablesAssignment") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Sample data generation
def generate_transaction_data(num_records=10000):
    categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty"]
    regions = ["North", "South", "East", "West", "Central"]
    
    data = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_records):
        transaction_id = f"TXN_{i:06d}"
        customer_id = f"CUST_{random.randint(1, 2000):05d}"
        product_category = random.choice(categories)
        region = random.choice(regions)
        amount = round(random.uniform(10.0, 500.0), 2)
        quantity = random.randint(1, 5)
        transaction_date = base_date + timedelta(days=random.randint(0, 365))

        data.append((transaction_id, customer_id, product_category, region,
                    amount, quantity, transaction_date))

    return data

# Create the dataset
transaction_data = generate_transaction_data()
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("region", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("transaction_date", DateType(), True)
])

df = spark.createDataFrame(transaction_data, schema)
df.cache()
df.show(10)
print(f"Total transactions: {df.count()}")

# Create reference data that would typically come from external sources
category_discounts = {
    "Electronics": 0.10,
    "Clothing": 0.15,
    "Books": 0.05,
    "Home": 0.08,
    "Sports": 0.12,
    "Beauty": 0.20
}

region_tax_rates = {
    "North": 0.08,
    "South": 0.06,
    "East": 0.09,
    "West": 0.07,
    "Central": 0.05
}

# Create broadcast variables for the dictionaries above
broadcast_discounts = spark.sparkContext.broadcast(category_discounts)
broadcast_tax_rates = spark.sparkContext.broadcast(region_tax_rates)

# Create a DataFrame from category_discounts with columns: category, discount_rate
data_discounts = [{"category": category, "discount_rate": discount} for category, discount in broadcast_discounts.value.items()]
df_discounts = spark.createDataFrame(data_discounts)
df_discounts.show()

# Create a DataFrame from region_tax_rates with columns: region, tax_rate
data_tax_rates = [{"region": region, "tax_rate": tax} for region, tax in broadcast_tax_rates.value.items()]
df_tax_rates = spark.createDataFrame(data_tax_rates)
df_tax_rates.show()

# Use left joins to add discount_rate and tax_rate columns
discount_start = time.time()
df_discounts_join = df.join(df_discounts, df.product_category == df_discounts.category, "left")
df_discounts_join.show()
discount_end = time.time()

tax_start = time.time()
df_tax_join = df.join(df_tax_rates, df.region == df_tax_rates.region, "left")
df_tax_join.show()
tax_end = time.time()

# Calculate the elapsed times
discount_elapsed_time = discount_end - discount_start
tax_elapsed_time = tax_end - tax_start

# final_price = amount * (1 - discount_rate) * (1 + 
def get_final_price(row):
    discount = broadcast_discounts.value.get(row.product_category, 0.0)
    tax_rate = broadcast_tax_rates.value.get(row.region, 0.0)
    final_price = row.amount * (1 - discount) * (1 + tax_rate)
    return (row.customer_id, row.product_category, row.region, row.amount, row.quantity, row.transaction_date, final_price)

# 1. Using broadcast variables with map() operations
rdd_start = time.time()
result_rdd = df.rdd.map(get_final_price)
result_df = result_rdd.toDF(["customer_id", "product_category", "region", "amount", "quantity", "transaction_date", "final_price"])
result_df.show()
rdd_end = time.time()

rdd_elapsed_time = rdd_end - rdd_start

# 2. Using regular DataFrame joins
# Time both approaches and note the difference
print("discount join elapsed time: {:.2f} seconds".format(discount_elapsed_time))
print("taxes join elapsed time: {:.2f} seconds".format(tax_elapsed_time))
print("rdd broadcast elapsed time: {:.2f} seconds".format(rdd_elapsed_time))

# Create accumulators for tracking different metrics
electronic_count = spark.sparkContext.accumulator(0)
north_count = spark.sparkContext.accumulator(0)

#  Create a function that processes each row and updates accumulators
def using_accumulator(row):

    if row.product_category == "Electronics":
        electronic_count.add(1)
    if row.region == "North":
        north_count.add(1)

# Apply the function to each row using foreach
df.foreach(using_accumulator)

# Print the final accumulator values

print("Total electronic products:", electronic_count.value)
print("Total products in the North:", north_count.value)
