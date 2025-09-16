from pyspark.sql import SparkSession # type: ignore
import time

# activate SparkSession
spark = SparkSession.builder.getOrCreate()


spark = SparkSession.builder.appName("TimingExample").getOrCreate()

# load parquet file into DataFrame
parquet_path = "results.parquet"
df_test = spark.read.parquet(csv_path, header = 'true', inferSchema='true', sep=";")
df_test.createOrReplaceTempView("pyspark_sql_df")

# Start timing
start_time = time.time()

# Aggregate directly in spark SQL
final_results = spark.sql("""
    SELECT
        city,
        MIN(temp) AS min_temp,
        MAX(temp) AS max_temp,
        AVG(temp) AS avg_temp
    FROM pyspark_sql_df
    GROUP BY city
""")

# Collect results to driver
output = final_results.collect()
end_time = time.time()

# Show results
final_results.show()

# Eslapsed time
eslapsed_time = end_time - start_time

# Print the results and elapsed time
print(final_results)
print(f"\nTime elapsed: {eslapsed_time:.2f} seconds")

# stop SparkSession
spark.stop()
