import dlt

@dlt.table(
    name = 'customer_stg'
)
def customer_stg():
  return (
    spark.read.table("catalog_za.source.customers")
  )
