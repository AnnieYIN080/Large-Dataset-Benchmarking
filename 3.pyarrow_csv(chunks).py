import time
import pyarrow.csv as csv
import pyarrow as pa
from collections import defaultdict
from tqdm import tqdm

def csv_city_stats(filename):
    agg = defaultdict(lambda: {'min': float('inf'), 'max': float('-inf'), 'total': 0.0, 'count': 0})
    
    with csv.open_csv(filename, read_options=csv.ReadOptions(column_names=['city', 'temp']),
                      parse_options=csv.ParseOptions(delimiter=';')) as reader:
        
        while True:
            batch = reader.read_next_batch()
            if batch is None:
                break
            table = pa.Table.from_batches([batch])
            cities = table.column('city').to_pylist()
            temps = table.column('temp').to_pylist()

            
            for city, temp in zip(cities, temps):
                try:
                    temp_f = float(temp)
                except (ValueError, TypeError):
                    continue  
                agg[city]['min'] = min(agg[city]['min'], temp_f)
                agg[city]['max'] = max(agg[city]['max'], temp_f)
                agg[city]['total'] += temp_f
                agg[city]['count'] += 1

    
    for city, stats in agg.items():
        if stats['count'] > 0:
            stats['mean'] = stats['total'] / stats['count']
        else:
            stats['mean'] = None
        del stats['total'], stats['count']
    return agg


start_time = time.time()
stats = csv_city_stats('1.results.csv')  
end_time = time.time()

print('Helsinki', stats.get('Helsinki'))
print('Guatemala City', stats.get('Guatemala City'), '\n')
print(f"Time elapsed: {end_time - start_time:.2f} seconds")
