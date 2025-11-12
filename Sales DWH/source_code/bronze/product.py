import dlt

@dlt.table(
    name='product_stg'
)
def product_stg():
    return spark.read.table("catalog_za.source.products")
