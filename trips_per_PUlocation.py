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
    .count() \
    .withColumnRenamed("count", "fhv_trips")

df_fhvhv = spark.read.parquet(filePath + "fhvhv_*.parquet") \
    .select(col("PULocationID")) \
    .groupBy("PULocationID") \
    .count() \
    .withColumnRenamed("count", "fhvhv_trips")

df_green = spark.read.parquet(filePath + "green_*.parquet") \
    .select(col("PULocationID")) \
    .groupBy("PULocationID") \
    .count() \
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

joined_df.show(265)

# Add joined dataframe to the location data csv
df_location_import = spark.read.csv("/user/s2645963/project/taxi_zone_lookup.csv", header="true", inferSchema="true")
df_location = df_location_import.select(col("LocationID").alias("PULocationID"), col("Borough"), col("Zone"))

# Join joined and location dataframe
df_print = joined_df.join(df_location, on="PULocationID", how="inner") \
    .select(col("PULocationID"), col("Borough"), col("Zone"), col("fhv_trips"), col("fhvhv_trips"), col("green_trips"), col("yellow_trips")) \
    .orderBy("PULocationID")
df_print.show(265)

# Write the dataframe to csv on HDFS
output = "/user/s2645963/project/output/" + "Trips_per_PULocationID"
df_print.write.mode("overwrite").csv(output)

