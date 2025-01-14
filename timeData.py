from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, hour, minute, lit, sum as sum_

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Taxi types and their corresponding timestamp columns
taxi_types = [["fhv", "pickup_datetime"], ["fhvhv", "pickup_datetime"], ["green", "lpep_pickup_datetime"], ["yellow", "tpep_pickup_datetime"]]

# Living location IDs

living_id = [6,23,99,118,172,214,9,16,64,175,15,53,73,92,171,192,252,253,81,254,259]
# Schema for the output dataframe
schema = StructType([
    StructField('DropOffLocationID', IntegerType(), True),
    StructField('LivingLocationID', IntegerType(), True),
    StructField('Nr of DropOffsFromLiv', IntegerType(), True),
    StructField('Year', StringType(), True),
])

# Create an empty dataframe with the schema
final_df = spark.createDataFrame([], schema)

# Define general file path
filePath = "/user/s2645963/project/processed_data/"
years = [f"{y:02d}" for y in range(19, 25)]

# Loop through years and taxi types
for year in years:
    current_year = "20" + year + "/"
    year_path = filePath + current_year

    # Create an empty dataframe for the year
    year_df = spark.createDataFrame([], schema)

    for taxi_type, timename in taxi_types:
        type_path = year_path + taxi_type + "_*.parquet"

        # Read data for current taxi type and year
        import_df = spark.read.parquet(type_path)

        # Filter and process data
        HouseToWork = (
            import_df.filter(col("PULocationID").isin(living_id))
            .filter(
                (hour(col(timename)).between(6, 8)) |  # Hours 6 to 8 (inclusive)
                ((hour(col(timename)) == 9) & (minute(col(timename)) == 0))  # Hour 9, minute 0
            )
            .groupBy("DOLocationID", "PULocationID")
            .count()
            .withColumnRenamed("DOLocationID", "DropOffLocationID")
            .withColumnRenamed("PULocationID", "LivingLocationID")
            .withColumnRenamed("count", "Nr of DropOffsFromLiv")
            .withColumn("Year", lit(year))  # Add year
        )

        # Union the current dataframe for this taxi type with the year dataframe
        year_df = year_df.union(HouseToWork)

    # Aggregate by DropOffLocationID and LivingLocationID across all taxi types
    year_df = (
        year_df.groupBy("DropOffLocationID", "LivingLocationID", "Year")
        .agg(sum_("Nr of DropOffsFromLiv").alias("Nr of DropOffsFromLiv"))
    )

    # Union the year_df with final_df to accumulate all years' data
    final_df = final_df.union(year_df)

# Write the final dataframe to output directory
output = "/user/s2779323/project/output/housework/"
final_df.coalesce(1).write.mode("overwrite").csv(output)

print("Saved output for the years:")
print(*years, sep=", ")
print("to file path: " + output)
