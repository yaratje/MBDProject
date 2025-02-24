# File generates the number of trips done by a taxi type with as pick up point a specific locationID
# Output is written to a directory on the HDFS, /user/s2645963/project/output/Trips_per_PULocationID/

from pyspark.sql import SparkSession
from pyspark.sql.functions import column as col

spark = SparkSession.builder.getOrCreate()

# Import the location description csv
df_location_import = spark.read.csv("/user/s2645963/project/taxi_zone_lookup.csv", header="true", inferSchema="true")

# Define general file path
filePath = "/user/s2645963/project/processed_data/"
years = [f"{y:02d}" for y in range(19, 25)]

for year in years:
    current_year = "20" + year + "/"
    year_path = filePath + current_year

    # Create dataframe per
    df_fhv = spark.read.parquet(year_path + "fhv_*.parquet") \
        .select(col("PULocationID")) \
        .groupBy("PULocationID") \
        .count() \
        .withColumnRenamed("count", "fhv_trips")

    df_fhvhv = spark.read.parquet(year_path + "fhvhv_*.parquet") \
        .select(col("PULocationID")) \
        .groupBy("PULocationID") \
        .count() \
        .withColumnRenamed("count", "fhvhv_trips")

    df_green = spark.read.parquet(year_path + "green_*.parquet") \
        .select(col("PULocationID")) \
        .groupBy("PULocationID") \
        .count() \
        .withColumnRenamed("count", "green_trips")

    df_yellow = spark.read.parquet(year_path + "yellow_*.parquet") \
        .select(col("PULocationID")) \
        .groupBy("PULocationID") \
        .count() \
        .withColumnRenamed("count", "yellow_trips")

    # Combine the dataframes in to one with output being:
    # Key = locationID, column = types and data = summed total of rides
    joined_df = df_fhv.join(df_fhvhv, on="PULocationID", how="inner") \
        .join(df_green, on="PULocationID", how="inner") \
        .join(df_yellow, on="PULocationID", how="inner") \

    # joined_df.show(265)

    # Take the location, borough and zone columns from the import csv
    df_location = df_location_import.select(col("LocationID").alias("PULocationID"), col("Borough"), col("Zone"))

    # Join joined and location dataframe
    df_print = joined_df.join(df_location, on="PULocationID", how="inner") \
        .select(col("PULocationID"), col("Borough"), col("Zone"), col("fhv_trips"), col("fhvhv_trips"), col("green_trips"), col("yellow_trips")) \
        .orderBy("PULocationID")
    # df_print.show(265)

    # Write the dataframe to csv on HDFS
    output = "/user/s2645963/project/output/Trips_per_PULocationID/" + current_year
    df_print.write.mode("overwrite").option("header", True).csv(output)
    print("Saved output for the year " + current_year + " to file path " + output)
