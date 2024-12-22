from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import LongType
import subprocess

spark = SparkSession.builder.appName("process_files").getOrCreate()

input_dir = "hdfs:/user/s2645963/project/data/2023/"
processed_dir = "hdfs:/user/s2645963/project/processed_data/2023/"         #pas deze aan :)

try:
    hdfs_ls = subprocess.check_output(["hdfs", "dfs", "-ls", input_dir], text=True)
    # gare chatgpt shit maar het werkt om like alle files te krijgen in een folder
    files = [line.split()[-1] for line in hdfs_ls.strip().split("\n")]
except Exception as e:
    print("der gaat weer is wat fout")
    print(e)
    files = []

#doe de PULocationID / DOLocationID type vervangen door long met .withcollumn
for file in files:
    try:
        df = spark.read.parquet(file)
        df1 = df.withColumn("PULocationID", col("PULocationID").cast(LongType())) \
                .withColumn("DOLocationID", col("DOLocationID").cast(LongType()))

        output = processed_dir + file.split("/")[-1]
        df1.write.mode("overwrite").parquet(output)

    except Exception as e:
        print(f"ja dat ging fout: {file}")
        print(e)
