import dlt
from pyspark.sql.functions import *

@dlt.table(comment="Bronze table for Amex Credit Cards")
def bronze():
    return (
        spark.read.format("csv").option("header", True)
        .load(f"dbfs:/mnt/raw/amex_credit_cards")
    )

@dlt.table(comment="Silver table for Amex Credit Cards")
def silver():
    df = dlt.read("bronze")
    return df.dropDuplicates().na.fill(0)
