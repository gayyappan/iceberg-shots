## Setup virtual environment for python under your working directory
```
cd FIN 
python -m venv --without-pip --system-site-packages finenv ---> if you don't wantto inherit any system wide packages
```
Using system wide packages seems to be an issue for iceberg and pyparsing.

## Install pyiceberg
``` cd FIN 
source ./finenv/bin/activate
./finenv/bin/pip install "pyiceberg[pyarrow,sql-postgres]"
```

Version of python
```
python --version
Python 3.10.12
```
