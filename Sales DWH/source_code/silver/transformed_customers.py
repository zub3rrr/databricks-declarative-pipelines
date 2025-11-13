import dlt
from pyspark.sql.functions import *

@dlt.table(
    name = 'transformed_customer'
)

def transformed_sales():
    df = spark.read.table("customer_stg")
    df = df.withColumn("customer_name",upper(col("customer_name")))
    return df