import dlt
from pyspark.sql.functions import *

dlt.create_streaming_table(
    name="products_za")


dlt.create_auto_cdc_flow(
    target="products_za",
    source="transformed_product",
    keys=["product_id"],
    sequence_by="last_updated",
    stored_as_scd_type=2
)