# File generates a row per location per taxi type per year

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import column as col, lit

spark = SparkSession.builder.getOrCreate()
taxi_types = [["fhv","pickup_datetime"], ["fhvhv","pickup_datetime"], ["green","lpep_pickup_datetime"], ["yellow","tpep_pickup_datetime"]]

living_ids = [502, 411, 407,212]

# Final dataframe with following schema:
# LocationID, Nr of PickUps, Nr of DropOffs, Year, Type
schema = StructType([
    StructField('LocationID', IntegerType(), True),
    StructField('Nr of PickUps', IntegerType(), True),
    StructField('Nr of DropOffs', IntegerType(), True),
    StructField('Year', IntegerType(), True),
    StructField("TimePU", StringType(), True),
    StructField("TimeDO", StringType(), True),
])
print_df = spark.createDataFrame([], schema)

# Define general file path
filePath = "/user/s2645963/project/processed_data/"
years = [f"{y:02d}" for y in range(19, 25)]
PUTime = [f"{h:02d}:{m:02d}:00" for h in range(6, 10) for m in range(60) if h < 9 or m == 0]


# Create a dataframe per year containing all types
for year in years:
    current_year = "20" + year + "/"
    year_path = filePath + current_year

    # Create a dataframe per taxi type
    for taxi_type, timename in taxi_types:
        type_path = year_path + taxi_type + "_*.parquet"
        import_df = spark.read.parquet(type_path)

        # Take DOLocationID and count so to form "Nr of Pickups" per LocationID for airports
        HouseToWork = (
            import_df.filter(col("PULocationID").isin(living_ids))
            .filter(col(timename).isin(PUTime))
            .groupBy("DOLocationID", "PULocationID")
            .count()
            .withColumnRenamed("DOLocationID", "DropOffLocationID")
            .withColumnRenamed("PULocationID", "LivingLocationID")
            .withColumnRenamed("count", "Nr of DropOffsFromLiv")
        )


# Write print dataframe to output directory
output = "/user/s2779323/project/output/housework/"
HouseToWork.write.mode("overwrite").csv(output)

print("Saved output for the years:")
print(*years, sep=", ")
print("to file path: " + output)