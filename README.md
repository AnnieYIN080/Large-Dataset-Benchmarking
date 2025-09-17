# Introduce tools and Potential issue occuring during analysis
# 1. Generation data file (One Billion Row)
Based on https://github.com/lvgalvao/One-Billion-Row-Challenge-Python/blob/main/src/create_measurements.py<br>

## Explanation 
1. The output file doesn't include header. 
2. The output is a csv file.
3. Need create Parquet file.
4. Tools in reference are demos, providing direction rather than solutions.

# 2.  Large dataset access
1- Streaming _ Python _ csv<br>
2.1 - Chunks _ Pandas _ csv <br>
2.2 - Chunks _ Pandas _ parquet <br>
3.1 - Pyarrow _ csv (chunk) <br>
3.2 - Pyarrow _ parquet <br>
4.1 - Polars_ csv <br>
4.2 - Polars_ parquet <br>
5.1 - DuckDB _ csv <br>
5.2 - DuckDB _ parquet <br>
6.1 - Dask _ csv <br>
6.2 - Dask _ parquet <br>
7.1 - Pyspark _ csv <br>
7.2 - Pyspark _ parquet <br>
tips: 1-5 are run in single instance, 6-7 are recommended to run in EMR (can run locally)<br>

Future Step<br>
https://github.com/ibis-project/ibis/tree/main/ibis <br>

Ibis is a unified Python DataFrame API that enables seamless switching of the same piece of code among nearly 20 backends such as DuckDB, Polars, and BigQuery. <br>

* Build an isolated environment (miniconda or mamba);


        mamba create -n ibis-dev python=3.11 -y
        mamba activate ibis-dev

* Install ibis-framework and the required backend extensions;
* Run the sample or test script.
## Background
### For pure python and parquet file
Python itself does not have the built-in ability to directly operate Parquet files. That is to say, without installing any third-party libraries, Python cannot directly read, write or stream Parquet files.<br>

### For pyarrow with csv file
_Parquet does not require explicit chunk control, while CSV must be controlled by chunks._<br>

Parquet file natural Row Groups design:<br>
The Parquet format is divided into multiple "row groups" within the file, and each group is a relatively independently stored block. When pyarrow.parquet reads, it can directly iterate by these row groups, similar to chunk reading, without the need to manually specify the chunk size; Reading itself has the concept of partitioning, making it easy to handle large files.<br>

CSV is a full line of text without a built-in block structure:<br>
CSV files are simple line-by-line text without block indexing or compression or encoding mechanisms. When reading CSV files, if the file is large, to avoid memory explosion, it is necessary to implement row-by-row and block-by-block reading by oneself, and manually split into chunks to control memory usage. It is not possible to directly access the specified block as Parquet naturally supports block reading.<br>

## Challenges when doing analysis
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


# repeated test in bash
        for i in {1..10}
        do
          echo "Round $i:" >> log.txt
          python 1.python(streaming)_csv.py >> log.txt
          echo "====================" >> log.txt
        done
