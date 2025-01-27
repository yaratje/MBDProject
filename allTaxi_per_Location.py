# Takes all available data per year and combines it
# Used for the analysis compared to demographic data

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
])
print_df = spark.createDataFrame([], schema)

# Define general file path
filePath = "/user/s2645963/project/processed_data/"
years = [f"{y:02d}" for y in range(11, 25)]

# Create a dataframe per year containing all types
for year in years:
    current_year = "20" + year
    year_path = filePath + current_year + "/*"
    import_df_null = spark.read.parquet(year_path)
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

    # Add joined dataframe to the print dataframe with union so the rows get added
    print_df = joined_df.union(print_df)

# Testing purposes
print_df.orderBy(col("LocationID")).show(150)

# Write print dataframe to output directory
output = "/user/s2645963/project/output/allTaxi_per_location/"
print_df.coalesce(1).write.mode("overwrite").csv(output)

print("Saved output for the years:")
print(*years, sep=", ")
print("to file path: " + output)