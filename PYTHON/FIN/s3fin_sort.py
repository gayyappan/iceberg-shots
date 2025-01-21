import datetime
import os

import yfinance as yf
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema 
from pyiceberg.partitioning import PartitionField, PartitionSpec 
from pyiceberg.types import * 
from pyiceberg.table import *
from pyiceberg.table.sorting import SortOrder , SortField 
from pyiceberg.transforms import IdentityTransform
from pyiceberg import *
import pyarrow as pa
import pandas as pd
# Set up AWS credentials
os.environ['AWS_ACCESS_KEY_ID'] = 'xxxx'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'xxxx'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

#### this works if writining to a usual S3 bucket. Table bucket does not work
#### use pyiceberg_s3.yaml for s3 settings
'''
create table icefin.ticker(tick string,
  Open double,
  High double,
  Low double,
  Close double,
  Volume long,
  Dividends double,
  Stock Splits double,
  Date_new timestamp ) 
'''
def create_table( ):
	pgcatalog = load_catalog("postgres_catalog")
	pgcatalog.create_namespace_if_not_exists("icefin")
	ns = pgcatalog.list_namespaces()
	bid_schema = Schema(
       	 NestedField(field_id=1, name="tick", field_type=StringType(), required=False),
       		NestedField(field_id=2, name="Open", field_type=DoubleType(), required=False),
       	 NestedField(field_id=3, name="High", field_type=DoubleType(), required=False),
       	 NestedField(field_id=4, name="Low", field_type=DoubleType(), required=False),
       	 NestedField(field_id=5, name="Close", field_type=DoubleType(), required=False),
       	 NestedField(field_id=6, name="Volume", field_type=LongType(), required=False),
       	 NestedField(field_id=7, name="Dividends", field_type=DoubleType(), required=False),
       	 NestedField(field_id=8, name="Stock Splits", field_type=DoubleType(), required=False),
       	 NestedField(field_id=9, name="Date_new", field_type=TimestampType(), required=False),
    )
	bid_partition_spec = PartitionSpec(
       	 PartitionField(source_id=1, field_id = 1000, transform=IdentityTransform(), name="tick"),
    )
	bid_sortorder = SortOrder(
       	 SortField(source_id=9, transform=IdentityTransform() )
    )
###https://py.iceberg.apache.org/reference/pyiceberg/catalog/#pyiceberg.catalog.Catalog.create_table_if_not_exists
	bid_tab = pgcatalog.create_table_if_not_exists(
	    identifier="icefin.tic2",
	    schema=bid_schema,
	   partition_spec=bid_partition_spec,
           sort_order = bid_sortorder,
	)


def insert_table( sym: str, parqfile: str) :
	pgcatalog = load_catalog("postgres_catalog", **{"downcast-ns-timestamp-to-us-on-write": True})
	##bid_tab = pgcatalog.load_table("icefin.ticker")
	bid_tab = pgcatalog.load_table("icefin.tic2")
	tick_arrow_table = pa.parquet.read_table(parqfile)
	nr = tick_arrow_table.num_rows
	tick_lst = [sym] * nr
	tick_fld = pa.field('tick', pa.string())
	new_tick_arrow_table = tick_arrow_table.add_column(0, tick_fld, pa.array(tick_lst))
	##df = tick_arrow_table.to_pandas()
	### add "tick" column as first one
	###df.insert(0, "tick", sym)
###	df["tick"]= df["tick"].astype('string')
###	dftab = pa.Table.from_pandas(df) 
	bid_tab.append(new_tick_arrow_table)

def read_table():
        pgcatalog = load_catalog("postgres_catalog", **{"downcast-ns-timestamp-to-us-on-write": True})
        bid_tab = pgcatalog.load_table("icefin.tic2")
        print(bid_tab.scan().to_arrow().to_string(preview_cols=10))


##iceberg does not support ns , need us for timestamp
os.environ['PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE'] = 'true'
create_table( )
insert_table("AAPL", "/home/gayathri/PYTHON/FIN/marketdata/AAPL.parquet")
read_table()
