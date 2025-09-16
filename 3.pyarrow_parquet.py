import time
import pyarrow.parquet as pq
import pyarrow as pa
from collections import defaultdict
from tqdm import tqdm

def parquet_city_stats(filename):
    city_stats = {}
    # Initialize the aggregation statistics with defaultdict
    agg = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'total': 0.0, 'count': 0})

    parquet_file = pq.ParquetFile(filename)
    for batch in tqdm(parquet_file.iter_batches(batch_size=100_000), desc="Processing batches"):
    # for batch in parquet_file.iter_batches(batch_size=100_000):
        table = pa.Table.from_batches([batch])
        cities = table.column('city').to_pylist()
        temps = table.column('temp').to_pylist()
        for city, temp in zip(cities, temps):
            if temp is None:
                continue
            agg[city]['min'] = min(agg[city]['min'], temp)
            agg[city]['max'] = max(agg[city]['max'], temp)
            agg[city]['total'] += temp
            agg[city]['count'] += 1

    # Calculate the Mean
    for city, stats in agg.items():
        if stats['count'] > 0:
            stats['mean'] = stats['total'] / stats['count']
        else:
            stats['mean'] = None
        # Optional: Remove the "total" and "count" fields
        del stats['total'], stats['count']
    return agg

# Main execution
start_time = time.time()
stats = parquet_city_stats('1.results.parquet')
end_time = time.time()

print('Helsinki',stats['Helsinki'])
print('Guatemala City,',stats['Guatemala City'],'\n')

print(f"Time elapsed: {end_time - start_time:.2f} seconds")
