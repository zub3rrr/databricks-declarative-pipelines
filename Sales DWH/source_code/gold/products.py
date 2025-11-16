import dlt
from pyspark.sql.functions import *

@dlt.table(
    comment="SCD2 merge table for product price changes.",
    name="products_history")
def products_history():
    src = spark.read.table("transformed_product").withColumn(
        "start_date", current_timestamp()
    ).withColumn(
        "end_date", lit(None).cast("timestamp")
    )

    target = "product_history"
    spark.sql(f"""
        MERGE INTO {target} AS tgt
        USING (SELECT * FROM transformed_product) as src
        ON tgt.product_id = src.product_id 
        WHEN MATCHED AND tgt.price <> src.price THEN
          UPDATE SET
            tgt.start_date = src.last_updated,
            tgt.end_date = null
        WHEN NOT MATCHED THEN
          INSERT (
            product_id,
            product_name,
            product_category,
            price,
            last_updated,
            start_date,
            end_date
          )
          VALUES (
            src.product_id,
            src.product_name,
            src.product_category,
            src.price,
            src.last_updated,
            src.last_updated,
            NULL
          )
    """)

    return spark.read.table(target)