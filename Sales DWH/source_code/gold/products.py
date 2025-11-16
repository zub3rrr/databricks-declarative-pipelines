# import dlt
# from pyspark.sql.functions import *


# @dlt.table(
#     comment="Product SCD2 History Table",
#     name = "products_history"
# )
# def products_history():
#     return (
#         spark.createDataFrame([], """
#             product_id STRING,
#             product_name STRING,
#             product_category STRING,
#             price DECIMAL(10,2),
#             last_updated TIMESTAMP,
#             start_date TIMESTAMP,
#             end_date TIMESTAMP,
#             current_flag INT
#         """)
#     )


# def scd_type():
#     status = spark.sql("""
#         MERGE INTO products_history AS tgt
#         USING (SELECT * FROM transformed_product) AS src
#         ON tgt.product_id = src.product_id
#         WHEN MATCHED AND tgt.price <> src.price THEN
#           UPDATE SET
#             tgt.start_date = src.last_updated,
#             tgt.end_date = NULL
#         WHEN NOT MATCHED THEN
#           INSERT (product_id, product_name, product_category, price, last_updated, start_date, end_date)
#           VALUES (src.product_id, src.product_name, src.product_category, src.price, src.last_updated, src.last_updated, NULL)
#     """)
#     return status
  

# scd_type()