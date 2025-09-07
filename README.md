# Introduce tools and Potential issue occuring during analysis
# 1. Generation data file (One Billion Row)
Based on https://github.com/lvgalvao/One-Billion-Row-Challenge-Python/blob/main/src/create_measurements.py<br>

## Explanation 
1. The output file doesn't include header. 
2. The output is a csv file.
3. Need create Parquet file.
4. Tools in reference are demos, providing direction rather than solutions.

## Challenges 
- Pandas doesn't support index. It cannot print the specific city.
- Dask has lots of error msg, but no error msg when put header inside.
- When read .csv file in DUCKDB, if there's no header, it should be added or defined when reading.
      FROM read_csv('results.csv', delim=';', columns = {'city': 'VARCHAR', 'temp': 'DOUBLE'})
- Parquet file cannot be written by command below.
      with open("results.parquet", "w") as file:
The file is not a parquet format: The file provided might actually be in CSV, TXT or other formats, but the file extension has been changed to.parquet.<br>
Parquet files have specific file headers and file tails "magic bytes", which DuckDB uses to determine the file format.<br>
Parquet: write tool generation (such as pandas' to_parquet(), pyarrow, spark, etc.)<br>
Parquet can be read in pandas, but due to data is too large kernel will break down.<br>
- Exploring more on tools, can add Spark to try

## Solutions
1. Adding or defining header when created data in generating rather than analysing
2. Producing parquet file by using pyarrow to input data directly 

# Analysis
Number of cities<br>
1- Pure Python _ csv<br>
2- Pandas _ csv <br>
2- Pandas _ parquet <br>
3.1 - Dask _csv <br>
3.2 - Dask _ parquet <br>
3.3 - Pyarrow _parquet <br>
3.4 - Pyarrow _csv <br>
4.1 - Polars_csv <br>
4.2 - Polars_parquet <br>
5.1 - DuckDB _csv <br>
5.2 - DuckDB _parquet <br>
6.1 Pyspark_csv <br>
6.2 Pyspark_parquet <br>
Future Step<br>
