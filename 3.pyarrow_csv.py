import time
import pyarrow.csv as pv
import pyarrow as pa
from collections import defaultdict
from tqdm import tqdm


def csv_city_stats(filename, batch_size=100_000):
    agg = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'total': 0.0, 'count': 0})

    # Set the byte size for batch reading and control the amount of a single read
    read_options = pv.ReadOptions(block_size=batch_size)

    # Open the CSV file stream and support block reading
    csv_reader = pv.open_csv(filename, read_options=read_options)

    for batch in tqdm(csv_reader, desc="Processing batches"):
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

    # Calculate the mean and clean up the temporary fields
    for city, stats in agg.items():
        if stats['count'] > 0:
            stats['mean'] = stats['total'] / stats['count']
        else:
            stats['mean'] = None
        del stats['total'], stats['count']

    return agg


# Main program execution
start_time = time.time()
stats = csv_city_stats('results.csv')
end_time = time.time()

print('Helsinki', stats.get('Helsinki'))
print('Guatemala City', stats.get('Guatemala City'))
print(f"Time elapsed: {end_time - start_time:.2f} seconds")
