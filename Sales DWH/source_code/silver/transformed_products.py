import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

@dlt.table(
    name = 'transformed_product'
)

def transformed_sales():
    df = spark.read.table("product_stg")
    df = df.withColumn("price",col("price").cast(IntegerType()))
    return df