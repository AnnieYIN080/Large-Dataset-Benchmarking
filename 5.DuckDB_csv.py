import duckdb
import time

con = duckdb.connect()

# Start time tracking
start_time = time.time()

# Aggregate directly in DuckDB
final_results = con.execute("""
    SELECT
        city,
        MIN(temp) AS min_temp,
        MAX(temp) AS max_temp,
        AVG(temp) AS avg_temp
    FROM read_csv(
        'results.csv',
        columns = {'city': 'VARCHAR', 'temp': 'DOUBLE'}, delim=';'
    )
    GROUP BY city
""").fetchdf()

# End time tracking and calculate elapsed time
end_time = time.time()
elapsed_time = end_time - start_time

# Print the results and elapsed time
print(final_results)
print(f"\nTime elapsed: {elapsed_time:.2f} seconds")

con.close()
