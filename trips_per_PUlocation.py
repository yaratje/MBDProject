# Attempt to read from the parquet files
# First try to read 2024

from pyspark.sql import SparkSession
from pyspark.sql.functions import column as col, desc

spark = SparkSession.builder.getOrCreate()

# Define general file path
filePath = "/user/s2645963/project/processed_data/2024/"

# Create dataframe per
df_fhv = spark.read.parquet(filePath + "fhv_*.parquet") \
    .select(col("PULocationID")) \
    .groupBy("PULocationID") \
    .count().alias("fhv_trips") \
    .withColumnRenamed("count", "fhv_trips")

df_fhvhv = spark.read.parquet(filePath + "fhvhv_*.parquet") \
    .select(col("PULocationID")) \
    .groupBy("PULocationID") \
    .count().alias("fhvhv_trips") \
    .withColumnRenamed("count", "fhvhv_trips")

df_green = spark.read.parquet(filePath + "green_*.parquet") \
    .select(col("PULocationID")) \
    .groupBy("PULocationID") \
    .count().alias("green_trips") \
    .withColumnRenamed("count", "green_trips")

df_yellow = spark.read.parquet(filePath + "yellow_*.parquet") \
    .select(col("PULocationID")) \
    .groupBy("PULocationID") \
    .count() \
    .withColumnRenamed("count", "yellow_trips")

# Combine the dataframes in to one with output being:
# Key = locationID, column = types and data = summed total of rides
joined_df = df_fhv.join(df_fhvhv, on="PULocationID", how="inner") \
    .join(df_green, on="PULocationID", how="inner") \
    .join(df_yellow, on="PULocationID", how="inner") \
    .orderBy("PULocationID")

joined_df.show(265)

