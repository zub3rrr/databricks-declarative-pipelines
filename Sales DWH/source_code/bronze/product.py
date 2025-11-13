import dlt


product_rules = {
  "rule-1":"product_id IS NOT NULL",
  "rule-2":"price >= 0"
}

@dlt.table(
    name='product_stg'
)
@dlt.expect_all_or_fail(product_rules)
def product_stg():
    return spark.read.table("catalog_za.source.products")
