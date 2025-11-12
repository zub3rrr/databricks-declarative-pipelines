import dlt
from pyspark.sql.functions import *

@dlt.table(
    name='load_east_region_sales'
)
def load_east_region_sales():
    return spark.read.table("catalog_za.source.sales_east")

@dlt.table(
    name='load_west_region_sales'
)
def load_west_region_sales():
    return spark.read.table("catalog_za.source.sales_west")

@dlt.table(
    name='combined_sales'
)
def combined_sales():
    east = spark.read.table("load_east_region_sales")
    west = spark.read.table("load_west_region_sales")
    combined = east.unionByName(west)
    return combined



"""
Alternative approach to combine tables - in stream table apart from above approach is to use append flow
For-Eg:
dlt.create_table( name = 'append_sales' ) 

@dlt.append_flow(target = 'append_sales') 
def load_east_region_sales(): 
    return spark.read.table("catalog_za.source.sales_east") 
    
@dlt.append_flow(target= = 'append_sales') 
def load_west_region_sales(): 
    return spark.read.table("catalog_za.source.sales_west")

Note: Appendflow is not supported in batch load
Because:
    When you use dlt.create_table() in batch mode, you’re creating a static table. This means:

    The table represents a complete dataset that’s rewritten or replaced each time you load data.

    There’s no concept of incremental updates or continuous appends because batch processing assumes all data is available at once.

    In contrast, @dlt.append_flow() relies on streaming semantics. That means:

    It expects data to arrive continuously or incrementally over time.

    The system must track state — i.e., what’s already been processed and what’s new.

    Streaming tables maintain metadata to support incremental ingestion, deduplication, and exactly-once processing.

    So, if you’re using a batch table (via dlt.create_table()), there’s no streaming state or incremental tracking, and therefore, @dlt.append_flow() isn’t supported — it has nothing to “append to” in a streaming sense.

"""