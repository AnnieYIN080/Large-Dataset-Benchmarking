import dask.dataframe as dd
import time
from dask.distributed import Client
# Dask can and manage sub-processes normally (if meeting error can choose freeze_support)
from multiprocessing import freeze_support

def process_data_dask(file_path):
    start_time = time.time()
    client = Client()
    
    # Read parquet directly without specifying delimiters, headers, or dtypes
    ddf = dd.read_parquet(file_path)  
    
    results = ddf.groupby('city')['temp'].agg(['min', 'max', 'mean']).rename(columns={
        'min': 'temperature_min',
        'max': 'temperature_max',
        'mean': 'temperature_mean'
    })
    final_results = results.compute()
    end_time = time.time()
    print(f"Elapsed Time (Dask): {end_time - start_time:.2f} seconds")
    client.close()
    return final_results

if __name__ == '__main__':
    freeze_support()  
    file_path = 'results.parquet'
    print("Start Processing")
    city_stats_dask = process_data_dask(file_path)
    print("The processing is completed, and the results are as follows:")
    print(city_stats_dask.head())
