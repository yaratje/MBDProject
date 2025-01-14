# File generates a row per location per taxi type per year

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import column as col, lit

spark = SparkSession.builder.getOrCreate()

airport_ids = [1, 132, 138]

# Final dataframe with following schema:
# LocationID, Nr of PickUps, Nr of DropOffs, Year, Type
schema = StructType([
    StructField('LocationID', IntegerType(), True),
    StructField('LocationID2', IntegerType(), True),
    StructField('Nr of DropOffs/pickupp', IntegerType(), True),
])
print_df = spark.createDataFrame([], schema)
print_df2 = spark.createDataFrame([], schema)

# Define general file path
filePath = "/user/s2645963/project/processed_data/"
years = [f"{y:02d}" for y in range(19, 25)]
taxi_types = ["fhv", "fhvhv", "green", "yellow"]

# Create a dataframe per year containing all types
for year in years:
    current_year = "20" + year + "/"
    year_path = filePath + current_year

    # Create a dataframe per taxi type
    for taxi_type in taxi_types:
        type_path = year_path + taxi_type + "_*.parquet"
        import_df = spark.read.parquet(type_path)

        # Take PULocationID and count so to form "Nr of Pickups" per LocationID for airports
        pickups_to_airports = (
            import_df.filter(col("DOLocationID").isin(airport_ids))  # Filter trips to airports
            .groupBy("PULocationID", "DOLocationID")
            .count() 
            .withColumnRenamed("PULocationID", "PickupLocationID")
            .withColumnRenamed("DOLocationID", "AirportLocationID")
            .withColumnRenamed("count", "Nr of PickUpsToAirport")
        )

        # Take DOLocationID and count so to form "Nr of Pickups" per LocationID for airports
        dropoffs_from_airports = (
            import_df.filter(col("PULocationID").isin(airport_ids))  # Filter trips from airports
            .groupBy("DOLocationID", "PULocationID")
            .count()
            .withColumnRenamed("DOLocationID", "DropOffLocationID")
            .withColumnRenamed("PULocationID", "AirportLocationID")
            .withColumnRenamed("count", "Nr of DropOffsFromAirport")
        )

        print_df = pickups_to_airports.union(print_df)
        print_df2 = dropoffs_from_airports.union(print_df2)

# Write print dataframe to output directory
output = "/user/s2779323/project/output/Trips_per_LocationID_airport/"
print_df.coalesce(1).write.mode("overwrite").csv(output + "/pickup_output")
print_df2.coalesce(1).write.mode("overwrite").csv(output+ "/dropoff_output")

print("Saved output for the years:")
print(*years, sep=", ")
print("to file path: " + output)