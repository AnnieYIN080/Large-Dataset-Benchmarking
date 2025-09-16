import pandas as pd
import time
from tqdm import tqdm

def process_large_data(file_path):
    start_time = time.time()
    # Read the parquet file using dask
    ddf = dd.read_parquet(file_path)

    # Calculate the statistics
    results = ddf.groupby('city')['temp'].agg(['min', 'max', 'mean']).compute().rename(columns={
        'min': 'temperature_min',
        'max': 'temperature_max',
        'mean': 'temperature_mean'
    })

    end_time = time.time()  # End timing
    elapsed_time = end_time - start_time  # Calculate elapsed time

    print(f"Elapsed Time: {elapsed_time:.2f} seconds")  # Print the elapsed time

    return results

# Usage
try:
    file_path = 'results.parquet'
    city_stats = process_large_data(file_path)
    print(city_stats)
except Exception as e:
    print(f"Error: {str(e)}")
