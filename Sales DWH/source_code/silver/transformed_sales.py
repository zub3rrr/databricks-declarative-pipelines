import dlt
from pyspark.sql.functions import *

@dlt.table(
    name = 'transformed_sales'
)

def transformed_sales():
    df = spark.read.table("sales_stg")
    df = df.withColumn("total_amount", col("amount") * col("quantity"))
    return df