from pyspark.sql import SparkSession
from pyspark.sql.functions import column as col, desc
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.getOrCreate()

filePath = "/user/s2779323/project/processed_data/2023/yellow_*.parquet"

# Idea for force passing the schema I want to the parquet files but gave the same error that it
# Can't cast from INT32 to bigint (float)
schema = StructType([StructField("PULocationID", LongType(), True),])

df1 = spark.read.schema(schema).parquet(filePath).select(col("PULocationID")) \
    .filter(col("PULocationID").isNotNull()) \
    .groupby(col("PULocationID")).count() \
    .orderBy(desc(col("PULocationID")))

df1.printSchema()
df1.show()
