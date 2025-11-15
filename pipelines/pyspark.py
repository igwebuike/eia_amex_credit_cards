from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("eia_amex_credit_cards").getOrCreate()

df = spark.read.csv("input.csv", header=True, inferSchema=True)
df = df.dropDuplicates().na.fill(0)

df.write.mode("overwrite").parquet("output/amex_credit_cards")
print("PySpark ETL complete for Amex Credit Cards")
