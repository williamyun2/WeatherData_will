# OUTPUT IS PKL
# # OUTPUT IS PKL
# # OUTPUT IS PKL
# # OUTPUT IS PKL
# # OUTPUT IS PKL
# # OUTPUT IS PKL
# # OUTPUT IS PKL
# # OUTPUT IS PKL
# # OUTPUT IS PKL



import pandas as pd
import struct
from typing import Callable

def to_str(x, lens) -> str:
    if (x // 100) == 0:      
        return f"{x:.2f}".zfill(lens)
    elif ((-x) // 100) == 0: 
        return f"{x:.2f}".zfill(lens + 1)
    else:                    
        return f"{x:.2f}"

# Load station data
# station = pd.read_parquet("station_clean.parquet")
station = pd.read_parquet("hawaii_station.parquet")

# Clean and prepare station data
station['Region'] = station['Region'].fillna('')
station['Country2'] = station['Country2'].fillna('')
station['ElevationMeters'] = station['ElevationMeters'].astype(int)
station['Region'] = station['Region'].astype(str)
station['Country2'] = station['Country2'].astype(str)
station.reset_index(inplace=True)

# Create station identifier
station['WhoAmI'] = '+' + station['Latitude'].apply(to_str, args=(5,)) + station['Longitude'].apply(to_str, args=(6,)) + '/'
station['WhoAmI'].drop_duplicates(inplace=True)
station.sort_values(by=["Latitude", "Longitude"], inplace=True)

# Create null-terminated strings for binary format
to_cstring: Callable[[str], str] = lambda s: s.encode('ascii', 'replace') + b'\x00'
station['ascii_null_terminated_WhoAmI'] = station['WhoAmI'].apply(to_cstring)   
station['ascii_null_terminated_Region'] = station['Region'].apply(to_cstring) 
station['ascii_null_terminated_Country2'] = station['Country2'].apply(to_cstring)

# Final sort and type conversion
station.sort_values(by=["Latitude", "Longitude"], inplace=True)
station = station.astype({"Latitude": "double", "Longitude": "double", "ElevationMeters": "int16"})

# Write binary station file
with open("hawaii_era5_station.pkl", "wb") as file:
    for row in station.index:
        file.write(struct.pack('<d', station['Latitude'][row]))          # Write Latitude (DOUBLE)
        file.write(struct.pack('<d', station['Longitude'][row]))         # Write Longitude (DOUBLE)
        file.write(struct.pack('<h', station['ElevationMeters'][row]))   # Write AltitudeM (INT16)
        file.write(station['ascii_null_terminated_WhoAmI'][row])         # Write Name (CSTRING)
        file.write(station['ascii_null_terminated_Country2'][row])       # Write Country (CSTRING)
        file.write(station['ascii_null_terminated_Region'][row])         # Write Region (CSTRING)

print(f"Successfully created era5_station.pkl with {len(station)} stations")
print("First few stations:")
print(station[['Latitude', 'Longitude', 'ElevationMeters', 'WhoAmI']].head())




# OUTPUT IS PKL