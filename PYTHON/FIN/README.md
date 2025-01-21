## Step 0: Activate virtual env
```
source ./finenv/bin/activate
```

## Step 1: Generate some Parquet data.
```
python download.py
```
This downloads some Parquet files from Yahoo finance.

## Step 2: Setup Postgres
We use a Postgres database for the Iceberg metastore. Setup a Postgres instance and create a database called
"pyice" on that instance. (This is used in .pyiceberg.yaml file)

## Step 3: Write the data out as Iceberg files
We use a Postgres database as our metastore. The data files are stored on local file system or on S3.
We have 2 .yml files to switch between a local warehouse (on the file system) and a remote warehouse (on S3).
Copy the version that you want to use to .pyiceberg.yaml

First, set the environment variables like so:
```
export PYTHONPATH=$HOME/PYTHON/FIN/finenv/lib/python3.10/site-packages:$PYTHONPATH
export PYICEBERG_HOME=$HOME/PYTHON/FIN --> the location of .pyiceberg.yaml
```

First, try this with a local warehouse.
Modify .pyiceberg_disk.yml to the correct directory for your warehouse.
Copy this to .pyiceberg.yaml

```
python localfin_sort.py
```

If you instead want the Iceberg data on S3, modify AWS credential info in s3fin_sort.py. Copy .pyiceberg_s3.yml to pyiceberg.yaml and setup the correct S3 prefix information.
```
python s3fin_sort.py
```

To get out of the virtual env
```
deactivate
```

## Step 4: Cleanup
`./cleanup.sh` --> clean up the entries in the Postgres catalog and the local file system (warehouse).
