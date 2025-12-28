import dlt
from pyspark.sql.functions import *

dlt.create_streaming_table(
    name="customers")


dlt.create_auto_cdc_flow(
    target="customers",
    source="transformed_customer",
    keys=["customer_id"],
    sequence_by="last_updated",
    stored_as_scd_type=2
)