# File generates a row per location per taxi type per year
# Used to visualize the raw dataset

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import column as col, lit

spark = SparkSession.builder.getOrCreate()

# Final dataframe with following schema:
# LocationID, Nr of PickUps, Nr of DropOffs, Year, Type
schema = StructType([
    StructField('LocationID', IntegerType(), True),
    StructField('Nr of PickUps', IntegerType(), True),
    StructField('Nr of DropOffs', IntegerType(), True),
    StructField('Year', StringType(), True),
    StructField('Type', StringType(), True),
])
print_df = spark.createDataFrame([], schema)

# Define general file path
filePath = "/user/s2645963/project/processed_data/"
years = [f"{y:02d}" for y in range(19, 25)]
taxi_types = ["fhvhv"] # "yellow", "fhv", "fhvhv", "green",

# Create a dataframe per year containing all types
for year in years:
    current_year = "20" + year
    year_path = filePath + current_year + "/"

    # Create a dataframe per taxi type
    for taxi_type in taxi_types:
        type_path = year_path + taxi_type + "_*.parquet"
        import_df_null = spark.read.parquet(type_path)
        import_df = import_df_null.fillna(-1)

        # Take PULocationID and count so to form "Nr of Pickups" per LocationID
        PULocationID_df = import_df.select(col("PULocationID").alias("LocationID")) \
            .groupBy("LocationID") \
            .count() \
            .withColumnRenamed("count", "Nr of PickUps")

        # Take DOLocationID and count so to form "Nr of DropOffs" per LocationID
        DOLocationID_df = import_df.select(col("DOLocationID").alias("LocationID")) \
            .groupBy("LocationID") \
            .count() \
            .withColumnRenamed("count", "Nr of DropOffs")

        # Join dataframes and add columns "Year" and "Type"
        joined_df = PULocationID_df.join(DOLocationID_df, on="LocationID", how="inner") \
            .withColumn("Year", lit(current_year)) \
            .withColumn("Type", lit(taxi_type))

        # Add joined dataframe to the print dataframe with union so the rows get added
        print_df = joined_df.union(print_df)

# Testing purposes
print_df.orderBy(col("LocationID")).show(150)

# Write print dataframe to output directory
output = "/user/s2645963/project/output/datasetVisualization/"
print_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output)

print("Saved output for the years:")
print(*years, sep=", ")
print("to file path: " + output)