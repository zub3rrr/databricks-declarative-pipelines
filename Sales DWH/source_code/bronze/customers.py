import dlt

customers_rule = {
"rule-1":"customer_id IS NOT NULL",
"rule-2":"customer_name IS NOT NULL",
}

@dlt.table(
    name = 'customer_stg'
)
@dlt.expect_all_or_fail(customers_rule)
def customer_stg():
  return (
    spark.read.table("catalog_za.source.customers")
  )
