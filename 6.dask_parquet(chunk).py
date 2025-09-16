import dask.dataframe as dd
import time
from tqdm import tqdm
from dask.distributed import Client

def process_data_dask(file_path, chunk_size=1000000):
    start_time = time.time()

    # Initialize a Dask client for parallel processing
    client = Client()

    # Load data with Dask, specifying chunk size for efficient partitioning
    ddf = dd.read_parquet(file_path, sep=';', header=0, names=['city', 'temp'], blocksize=chunk_size)

    # Group by 'city' and calculate aggregations
    # Note: Dask automatically parallelizes these operations
    results = ddf.groupby('city')['temp'].agg(['min', 'max', 'mean']).rename(columns={
        'min': 'temperature_min',
        'max': 'temperature_max',
        'mean': 'mean'
    })

    # Combine the results and compute the final DataFrame
    final_results = results.compute()
    # Rename the mean column to temperature_mean to be consistent with pandas implementation
    final_results = final_results.rename(columns={'mean': 'temperature_mean'})

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Elapsed Time (Dask): {elapsed_time:.2f} seconds")

    # Close the Dask Client to release resources
    client.close()

    return final_results

# Specify your file path
file_path = 'results.parquet'

# Start a Dask cluster and process data
city_stats_dask = process_data_dask(file_path)
print(city_stats_dask.head())
