from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.csv("s3://your-bucket/raw/amex_credit_cards.csv", header=True, inferSchema=True)
df = df.dropDuplicates().na.fill(0)

df.write.mode("overwrite").parquet("s3://your-bucket/processed/amex_credit_cards")
print("Glue ETL complete for Amex Credit Cards")
