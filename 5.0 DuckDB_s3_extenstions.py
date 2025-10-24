#!pip install s3fs httpfs duckdb duckdb-engine duckdb-extensions jupysql --quiet

import duckdb

# =======================================================================
# --- Method 1: Direct SQL Query to Pandas DataFrame ---

# Install the necessary extensions for accessing remote files (HTTPFS) and S3 (AWS).
duckdb.sql("install httpfs;")
duckdb.sql("install aws;")
# Load the installed extensions into the current session.
duckdb.sql("load httpfs;")
duckdb.sql("load aws;")

# Load AWS credentials from standard environments (e.g., IAM roles, environment variables).
# The .fetchall() is often used just to execute the command fully.
duckdb.sql("CALL load_aws_credentials()").fetchall()

data_path = "s3://dddddddddd.csv"

# Execute a SQL query to read the CSV file directly from S3 using the read_csv_auto function.
# The .df() method converts the DuckDB result set into an in-memory Pandas DataFrame.
sql_df = duckdb.sql(f"SELECT * FROM read_csv_auto('{data_path}')").df()

sql_df.head() # Display the first few rows of the Pandas DataFrame.

# =======================================================================
# --- Method 2: Using jupysql (%sql magic) ---

import duckdb

%load_ext sql # Load the SQL magic extension (jupysql).

# Install and load extensions again, specifically for the %sql connection environment.
duckdb.sql("install httpfs;")
duckdb.sql("install aws;")
duckdb.sql("load httpfs;")
duckdb.sql("load aws;")

'''
%config SqlMagic.autopolars=True  # Configuration options for SqlMagic (e.g., enabling automatic conversion to Polars).
%config SqlMagic.feedback=False
%config SqlMagic.displaycon=False
%config SqlMagic.named_parameters="enabled"
%config SqlMagic.displaylimit=100
'''
# Establish a connection to DuckDB using the SQL magic.
%sql duckdb:// 

# Load AWS credentials within the SQL magic environment.
%sql call load_aws_credentials() 

%sql rollback
  
%sql csv << SELECT * FROM read_csv_auto('s3://dddddddddd.csv')
# Alternative: Create a persistent table within the DuckDB session.
# %sql CREATE OR REPLACE TABLE table AS SELECT * FROM read_csv_auto('s3://dddddddddd.csv')

df_csv = csv.DataFrame() # Convert the SQL variable 'csv' into an in-memory Pandas DataFrame.
df_csv.head() # Display the first few rows.


# =======================================================================
# --- Method 3: Direct Python Connection (Multi-file Join Example) ---

import duckdb
import time

# Create a connection to an in-memory DuckDB database (default behavior).
con = duckdb.connect()
# con = duckdb.connect("database.duckdb") # Alternative: Connect to a file-backed database.

# Load AWS credentials using the connection object's execute method.
con.execute("call load_aws_credentials()")

'''
# These steps are often redundant if done globally but shown here for connection-specific setup.
con.execute("install httpfs;")
con.execute("install aws;")
con.execute("load httpfs;")
con.execute("load aws;")
con.execute("CALL load_aws_credentials()").fetchall()
'''
con.execute("rollback") # Ensure clean slate before DDL/DML operation.

# Execute a complex query: perform a JOIN across two separate CSV files hosted on S3,
# and persist the result into a new DuckDB table named 'table'.
con.execute(f"""
    CREATE OR REPLACE TABLE table AS
    SELECT *
    FROM read_csv_auto('s3://dddddddddd.csv') AS table_d
    JOIN read_csv_auto('s3://ffffffffff.csv') AS table_f
    ON table_d.id = table_f.id
""")

# Retrieve and display the schema (metadata) of the newly created table.
table_schema = con.execute(f'DESCRIBE table').fetchall()
table_schema

con.close() 
