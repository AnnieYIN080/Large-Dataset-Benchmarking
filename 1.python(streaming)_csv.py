import time
# Show progress bar function
from tqdm import tqdm

def read_and_calculate_stats(file_name):
    city_stats = {}
    with open(file_name, 'r') as file:
        # !!!!!!!!!!!!!!!
        header = next(file)  # Skip the header line
        # !!!!!!!!!!!!!!!
        for line in tqdm(file, desc="Processing data"):
            line = line.strip()  # Strip whitespace
            if not line or ';' not in line:  # Skip empty lines or lines without ;
                continue
            city, temp = line.split(';', maxsplit=1)  # Split only at first ;
            try:
                temp = float(temp)
            except ValueError:  # Handle invalid temperature values
                print(f"Warning: Invalid temperature value '{temp}' for city '{city}'")
                continue

            if city in city_stats:
                stats = city_stats[city]
                stats['count'] += 1
                stats['total'] += temp
                if temp < stats['min']:
                    stats['min'] = temp
                if temp > stats['max']:
                    stats['max'] = temp
            else:
                city_stats[city] = {
                    'min': temp,
                    'max': temp,
                    'total': temp,
                    'count': 1
                }

    # Calculate mean from total and count
    for city, stats in city_stats.items():
        stats['mean'] = stats['total'] / stats['count']
        del stats['total'], stats['count']  # Optional: Remove these if no longer needed

    return city_stats

# Main execution
start_time = time.time()
city_stats = read_and_calculate_stats('1.results.csv')
end_time = time.time()

print('Helsinki',city_stats['Helsinki'])
print('Guatemala City,',city_stats['Guatemala City'],'\n')

print(f"Time elapsed: {end_time - start_time:.2f} seconds")
