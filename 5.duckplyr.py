# Recommand who is familiar with R and Python (don't need SQL)
import duckplyr as dpl
import duckdb

import pyarrow as pa
import pyarrow.csv as pv

# Read big data from CSV files and create Arrow tables
ds = pv.read_csv('google_colab/test_dataset/mnist_train_small.csv')

# Convert the Arrow table to a DuckDB table
conn = duckdb.connect()

# Load the data using duckpylr and convert it into a DuckDB table
df = dpl.from_arrow_table(ds)

# Process data using operations in the dplyr style
result = df.filter(df.year == 2018) \
           .groupby(df.category) \
           .summarise(total=dpl.sum(df.amount)) \
           .sort('total', ascending=False) \
           .limit(10)

# show results
result.show()
