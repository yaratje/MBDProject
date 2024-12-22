# Attempt to read from the parquet files
# First try to read 202

# Problems found: Schemas don't fully alline for all

from pyspark.sql import SparkSession
from pyspark.sql.functions import column as col, desc
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.getOrCreate()

filePath = "/user/s2645963/project/data/2023/yellow_*.parquet"

# Idea for force passing the schema I want to the parquet files but gave the same error that it
# Can't cast from INT32 to bigint (float)
schema = StructType([StructField("PULocationID", LongType(), True),])

df1 = spark.read.schema(schema).parquet(filePath).select(col("PULocationID")) \
    .filter(col("PULocationID").isNotNull()) \
    .groupby(col("PULocationID")).count() \
    .orderBy(desc(col("PULocationID")))

df1.printSchema()
df1.show()


# First Idea below, also with same problem


# Define a struct to pass while reading

# Main file path to Gerwin's part of HDFS where data is stored
# filePath = ("/user/s2645963/project/data/")



# Create a dataframe per taxi type as they don't all have the same schema
# While creating, only keep PUlocationID (pick up location ID)
df_fhv = spark.read.parquet(filePath + "2023/fhv_*.parquet").select(col(""))

df_fhvhv = spark.read.parquet(filePath + "2023/fhvhv_*.parquet")

df_green = spark.read.parquet(filePath + "2023/green_*.parquet")

df_yellow = spark.read.parquet(filePath + "2023/yellow_*.parquet")


# Combine the dataframes in to one with output being:
# Key = locationID, column = types and data = summed total of rides



