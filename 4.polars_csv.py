import polars as pl
import time


def process_data_polars_streaming(file_path):
    start_time = time.time()

    # Create a lazy scan of the CSV file
    df = pl.scan_csv(file_path, separator=";", has_header=True, new_columns=["city", "temp"])

    # Define aggregations and group by 'city' with streaming enabled
    results = (
        df.lazy()
        .group_by("city")
        .agg(
            [
                pl.col("temp").min().alias("temperature_min"),
                pl.col("temp").max().alias("temperature_max"),
                pl.col("temp").mean().alias("temperature_mean"),
            ]
        )
        .collect(streaming=True)  # Enable streaming
    )

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed Time: {elapsed_time:.2f} seconds")
    return results

# Specify your file path
file_path = "results.csv"
city_stats = process_data_polars_streaming(file_path)
print(city_stats)
