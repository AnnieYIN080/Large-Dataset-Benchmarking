#!/usr/bin/env python
# https://github.com/codebeez/one-billion-row-challenge-python/tree/main/data
# http://www.apache.org/licenses/LICENSE-2.0

# Based on https://github.com/lvgalvao/One-Billion-Row-Challenge-Python/blob/main/src/create_measurements.py

# JY added a header to the file and changed to csv file format and parquet file format. Need to mention that parquet file format is not supported by pandas due to large data.

# pip install pyarrow
# /Library/Frameworks/Python.framework/Versions/3.12/bin/python3.12 /Users/annie/Downloads/算法学习/onebillionWITHcomparsion/1.1one_billion_rows.py 100
import sys
import random
import time
import os
import pyarrow as pa
import pyarrow.parquet as pq

# Define the list of meteorological station names directly here
WEATHER_STATION_NAMES = [
    "Abha", "Abidjan", "Abéché", "Accra", "Addis Ababa", "Adelaide", "Aden", "Ahvaz", "Albuquerque",
    "Alexandra", "Alexandria", "Algiers", "Alice Springs", "Almaty", "Amsterdam", "Anadyr", "Anchorage",
    "Andorra la Vella", "Ankara", "Antananarivo", "Antsiranana", "Arkhangelsk", "Ashgabat", "Asmara",
    "Assab", "Astana", "Athens", "Atlanta", "Auckland", "Austin", "Baghdad", "Baguio", "Baku", "Baltimore",
    "Bamako", "Bangkok", "Bangui", "Banjul", "Barcelona", "Bata", "Batumi", "Beijing", "Beirut", "Belgrade",
    "Belize City", "Benghazi", "Bergen", "Berlin", "Bilbao", "Birao", "Bishkek", "Bissau", "Blantyre",
    "Bloemfontein", "Boise", "Bordeaux", "Bosaso", "Boston", "Bouaké", "Bratislava", "Brazzaville",
    "Bridgetown", "Brisbane", "Brussels", "Bucharest", "Budapest", "Bujumbura", "Bulawayo", "Burnie",
    "Busan", "Cabo San Lucas", "Cairns", "Cairo", "Calgary", "Canberra", "Cape Town", "Changsha",
    "Charlotte", "Chiang Mai", "Chicago", "Chihuahua", "Chișinău", "Chittagong", "Chongqing", "Christchurch",
    "City of San Marino", "Colombo", "Columbus", "Conakry", "Copenhagen", "Cotonou", "Cracow", "Da Lat",
    "Da Nang", "Dakar", "Dallas", "Damascus", "Dampier", "Dar es Salaam", "Darwin", "Denpasar", "Denver",
    "Detroit", "Dhaka", "Dikson", "Dili", "Djibouti", "Dodoma", "Dolisie", "Douala", "Dubai", "Dublin",
    "Dunedin", "Durban", "Dushanbe", "Edinburgh", "Edmonton", "El Paso", "Entebbe", "Erbil", "Erzurum",
    "Fairbanks", "Fianarantsoa", "Flores,  Petén", "Frankfurt", "Fresno", "Fukuoka", "Gabès", "Gaborone",
    "Gagnoa", "Gangtok", "Garissa", "Garoua", "George Town", "Ghanzi", "Gjoa Haven", "Guadalajara",
    "Guangzhou", "Guatemala City", "Halifax", "Hamburg", "Hamilton", "Hanga Roa", "Hanoi", "Harare",
    "Harbin", "Hargeisa", "Hat Yai", "Havana", "Helsinki", "Heraklion", "Hiroshima", "Ho Chi Minh City",
    "Hobart", "Hong Kong", "Honiara", "Honolulu", "Houston", "Ifrane", "Indianapolis", "Iqaluit", "Irkutsk",
    "Istanbul", "İzmir", "Jacksonville", "Jakarta", "Jayapura", "Jerusalem", "Johannesburg", "Jos", "Juba",
    "Kabul", "Kampala", "Kandi", "Kankan", "Kano", "Kansas City", "Karachi", "Karonga", "Kathmandu",
    "Khartoum", "Kingston", "Kinshasa", "Kolkata", "Kuala Lumpur", "Kumasi", "Kunming", "Kuopio",
    "Kuwait City", "Kyiv", "Kyoto", "La Ceiba", "La Paz", "Lagos", "Lahore", "Lake Havasu City",
    "Lake Tekapo", "Las Palmas de Gran Canaria", "Las Vegas", "Launceston", "Lhasa", "Libreville",
    "Lisbon", "Livingstone", "Ljubljana", "Lodwar", "Lomé", "London", "Los Angeles", "Louisville",
    "Luanda", "Lubumbashi", "Lusaka", "Luxembourg City", "Lviv", "Lyon", "Madrid", "Mahajanga",
    "Makassar", "Makurdi", "Malabo", "Malé", "Managua", "Manama", "Mandalay", "Mango", "Manila",
    "Maputo", "Marrakesh", "Marseille", "Maun", "Medan", "Mek'ele", "Melbourne", "Memphis", "Mexicali",
    "Mexico City", "Miami", "Milan", "Milwaukee", "Minneapolis", "Minsk", "Mogadishu", "Mombasa",
    "Monaco", "Moncton", "Monterrey", "Montreal", "Moscow", "Mumbai", "Murmansk", "Muscat", "Mzuzu",
    "N'Djamena", "Naha", "Nairobi", "Nakhon Ratchasima", "Napier", "Napoli", "Nashville", "Nassau",
    "Ndola", "New Delhi", "New Orleans", "New York City", "Ngaoundéré", "Niamey", "Nicosia", "Niigata",
    "Nouadhibou", "Nouakchott", "Novosibirsk", "Nuuk", "Odesa", "Odienné", "Oklahoma City", "Omaha",
    "Oranjestad", "Oslo", "Ottawa", "Ouagadougou", "Ouahigouya", "Ouarzazate", "Oulu", "Palembang",
    "Palermo", "Palm Springs", "Palmerston North", "Panama City", "Parakou", "Paris", "Perth",
    "Petropavlovsk-Kamchatsky", "Philadelphia", "Phnom Penh", "Phoenix", "Pittsburgh", "Podgorica",
    "Pointe-Noire", "Pontianak", "Port Moresby", "Port Sudan", "Port Vila", "Port-Gentil", "Portland (OR)",
    "Porto", "Prague", "Praia", "Pretoria", "Pyongyang", "Rabat", "Rangpur", "Reggane", "Reykjavík",
    "Riga", "Riyadh", "Rome", "Roseau", "Rostov-on-Don", "Sacramento", "Saint Petersburg", "Saint-Pierre",
    "Salt Lake City", "San Antonio", "San Diego", "San Francisco", "San Jose", "San José", "San Juan",
    "San Salvador", "Sana'a", "Santo Domingo", "Sapporo", "Sarajevo", "Saskatoon", "Seattle", "Ségou",
    "Seoul", "Seville", "Shanghai", "Singapore", "Skopje", "Sochi", "Sofia", "Sokoto", "Split",
    "St. John's", "St. Louis", "Stockholm", "Surabaya", "Suva", "Suwałki", "Sydney", "Tabora", "Tabriz",
    "Taipei", "Tallinn", "Tamale", "Tamanrasset", "Tampa", "Tashkent", "Tauranga", "Tbilisi",
    "Tegucigalpa", "Tehran", "Tel Aviv", "Thessaloniki", "Thiès", "Tijuana", "Timbuktu", "Tirana",
    "Toamasina", "Tokyo", "Toliara", "Toluca", "Toronto", "Tripoli", "Tromsø", "Tucson", "Tunis",
    "Ulaanbaatar", "Upington", "Ürümqi", "Vaduz", "Valencia", "Valletta", "Vancouver", "Veracruz",
    "Vienna", "Vientiane", "Villahermosa", "Vilnius", "Virginia Beach", "Vladivostok", "Warsaw",
    "Washington, D.C.", "Wau", "Wellington", "Whitehorse", "Wichita", "Willemstad", "Winnipeg",
    "Wrocław", "Xi'an", "Yakutsk", "Yangon", "Yaoundé", "Yellowknife", "Yerevan", "Yinchuan",
    "Zagreb", "Zanzibar City", "Zürich"
]

def build_weather_station_name_list():
    return list(set(WEATHER_STATION_NAMES))

# This is for generating a CSV file with test data.
def build_test_data_csv(weather_station_names, num_rows_to_create):
    start_time = time.time()
    coldest_temp = -12.9
    hottest_temp = 49.9
    station_names_10k_max = random.choices(weather_station_names, k=10_000)
    batch_size = 10000 if num_rows_to_create >= 10000 else 1
    progress_step = max(1, (num_rows_to_create // batch_size) // 100)
    print("Creating the file. this will take a few minutes...")

    try:
        with open("1.results.csv", "w") as file:
            file.write("city;temp\n")
            for _ in range(0, num_rows_to_create // batch_size):
                batch = random.choices(station_names_10k_max, k=batch_size)
                prepped_deviated_batch = "\n".join(
                    [
                        f"{station};{random.uniform(coldest_temp, hottest_temp):.1f}"
                        for station in batch
                    ]
                )
                file.write(prepped_deviated_batch + "\n")

        sys.stdout.write("\n")
    except Exception as e:
        print("Something went wrong. Printing error info and exiting...")
        print(e)
        exit()

    end_time = time.time()
    elapsed_time = end_time - start_time
    file_size = os.path.getsize("1.results.csv")
    human_file_size = convert_bytes(file_size)

    print("CSV written successfully 1.results.csv")
    print(f"Final size:  {human_file_size}")
    print(f"Elapsed time: {format_elapsed_time(elapsed_time)}")


# This is for generating a Parquet file with test data.
def build_test_data_parquet(weather_station_names, num_rows_to_create, chunk_size=1_000_000):
    start_time = time.time()
    coldest_temp = -12.9
    hottest_temp = 49.9
    num_chunks = (num_rows_to_create + chunk_size - 1) // chunk_size
    parquet_file = "1.results.parquet"
    print("Creating the file. this will take a few minutes...")
    schema = pa.schema([
        ("city", pa.string()),
        ("temp", pa.float64())
    ])

    with pq.ParquetWriter(parquet_file, schema) as writer:
        for chunk_idx in range(num_chunks):
            current_chunk_size = min(chunk_size, num_rows_to_create - chunk_idx * chunk_size)
            city_array = pa.array(random.choices(weather_station_names, k=current_chunk_size), type=pa.string())
            temp_array = pa.array(
                [round(random.uniform(coldest_temp, hottest_temp), 1) for _ in range(current_chunk_size)],
                type=pa.float64()
            )
            table = pa.Table.from_arrays([city_array, temp_array], schema=schema)
            writer.write_table(table)
            print(f"Chunk {chunk_idx+1}/{num_chunks} written.")

    elapsed_time = time.time() - start_time
    file_size = os.path.getsize(parquet_file)
    human_file_size = convert_bytes(file_size)
    print(f"Parquet file written successfully: {parquet_file}")
    print(f"Final size:  {human_file_size}")
    print(f"Elapsed time: {format_elapsed_time(elapsed_time)}")


# (With underlined) is the number readability writing method supported by Python 3
def parse_args(args):
    default_num_rows = 1_000_000_000
    if len(args) > 1:
        try:
            num_rows = int(args[1].replace("_", ""))
            if num_rows <= 0:
                raise ValueError("Number of rows must be a positive integer.")
            return num_rows
        except ValueError as e:
            print(f"Error: {e}")
            print_usage()
            sys.exit(1)
    else:
        return default_num_rows

def print_usage():
    print("Usage: create_measurements.py [number of records]")
    print("       The number of records defaults to 1 billion if not specified.")
    print("       Use underscore notation for readability, e.g., 1_000_000.")


def confirm_creation(num_rows):
    confirm = input(
        f"Are you sure you want to create a file with {num_rows:,} rows? [y/n]: "
    )
    if confirm.lower() != "y":
        print("Operation cancelled by the user.")
        sys.exit()

# convert units 
def convert_bytes(num):
    for x in ["bytes", "KiB", "MiB", "GiB"]:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0


def format_elapsed_time(seconds):
    if seconds < 60:
        return f"{seconds:.3f} seconds"
    elif seconds < 3600:
        minutes, seconds = divmod(seconds, 60)
        return f"{int(minutes)} minutes {int(seconds)} seconds"
    else:
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if minutes == 0:
            return f"{int(hours)} hours {int(seconds)} seconds"
        else:
            return f"{int(hours)} hours {int(minutes)} minutes {int(seconds)} seconds"

# Just in case
def estimate_file_size(weather_station_names, num_rows_to_create):
    max_string = float("-inf")
    min_string = float("inf")
    per_record_size = 0
    record_size_unit = "bytes"

    for station in weather_station_names:
        if len(station) > max_string:
            max_string = len(station)
        if len(station) < min_string:
            min_string = len(station)
        per_record_size = ((max_string + min_string * 2) + len(",-123.4")) / 2

    total_file_size = num_rows_to_create * per_record_size
    human_file_size = convert_bytes(total_file_size)

    return f"The estimated file size is:  {human_file_size}.\n The final size will probably be much smaller (half)."


def main():
    # num_rows_to_create = parse_args(sys.argv)
    num_rows_to_create = 1_000_000
    weather_station_names = build_weather_station_name_list()
    estimated_size_message = estimate_file_size(
        weather_station_names, num_rows_to_create
    )
    print(estimated_size_message)
    confirm_creation(num_rows_to_create)
    build_test_data_parquet(weather_station_names, num_rows_to_create)
    build_test_data_csv(weather_station_names, num_rows_to_create)
    print("Finished creating test file!")


if __name__ == "__main__":
    main()
