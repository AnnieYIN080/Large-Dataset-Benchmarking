import dask.dataframe as dd
import time
from dask.distributed import Client


def process_data_dask(file_path, chunk_size=1000000):
    start_time = time.time()

    client = Client()

    ddf = dd.read_csv(file_path, sep=';', header=0, names=['city', 'temp'], blocksize=chunk_size)

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
    file_path = '/Users/annie/Downloads/1.results.csv'
    print("Start Processing")
    city_stats_dask = process_data_dask(file_path)
    print("The processing is completed, and the results are as follows:")
    print(city_stats_dask.head())
