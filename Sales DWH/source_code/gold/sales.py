import dlt
from pyspark.sql.functions import *

dlt.create_streaming_table(
    name="sales"
    )


dlt.create_auto_cdc_flow(
    target="sales",
    source="transformed_sales",
    keys=["sales_id"],
    sequence_by="sale_timestamp",
    stored_as_scd_type=2
)