#!/usr/bin/env python3
"""
Analyze Hawaii temperature data by elevation and location
"""

from PWW_to_NC import PWW_to_NC
import numpy as np
import pandas as pd

# Load your PWW file
pww_file = r"C:\class\code\service_testing\cds\data\pww\custom\Hawaii_2020-01-01_to_2025-09-30.pww"
print("📂 Loading Hawaii dataset...")
ds = PWW_to_NC(pww_file, offset_time=False)

# Get temperature and location data
temp_f = ds['temp_F_2m'].values  # Shape: (time, lat, lon)
lats = ds['latitude'].values
lons = ds['longitude'].values

print("\n" + "="*60)
print("🌡️  OVERALL TEMPERATURE STATISTICS")
print("="*60)
print(f"Minimum temperature: {np.nanmin(temp_f):.1f}°F")
print(f"Maximum temperature: {np.nanmax(temp_f):.1f}°F")
print(f"Mean temperature: {np.nanmean(temp_f):.1f}°F")
print(f"Median temperature: {np.nanmedian(temp_f):.1f}°F")

# Analyze by location (each grid point)
print("\n" + "="*60)
print("📍 TEMPERATURE BY LOCATION")
print("="*60)

# Get station data if available
try:
    station_file = "station/hawaii_station.parquet"
    stations = pd.read_parquet(station_file)
    print(f"Loaded {len(stations)} station locations\n")
except:
    print("Station file not found, using grid coordinates\n")
    stations = None

# Analyze each grid point
results = []
for i in range(temp_f.shape[1]):  # lat dimension
    for j in range(temp_f.shape[2]):  # lon dimension
        temps = temp_f[:, i, j]
        temps_clean = temps[~np.isnan(temps)]
        
        if len(temps_clean) > 0:
            lat = lats[i, j]
            lon = lons[i, j]
            
            results.append({
                'lat': lat,
                'lon': lon,
                'min_temp': np.min(temps_clean),
                'max_temp': np.max(temps_clean),
                'mean_temp': np.mean(temps_clean),
                'std_temp': np.std(temps_clean)
            })

df = pd.DataFrame(results)

# Find extreme locations
print("🥶 COLDEST LOCATIONS (likely high elevation):")
coldest = df.nsmallest(5, 'min_temp')
for idx, row in coldest.iterrows():
    print(f"  Lat: {row['lat']:6.2f}°, Lon: {row['lon']:7.2f}° | "
          f"Min: {row['min_temp']:5.1f}°F, Max: {row['max_temp']:5.1f}°F, "
          f"Mean: {row['mean_temp']:5.1f}°F")

print("\n🔥 HOTTEST LOCATIONS (likely coastal/leeward):")
hottest = df.nsmallest(5, 'max_temp')
for idx, row in hottest.iterrows():
    print(f"  Lat: {row['lat']:6.2f}°, Lon: {row['lon']:7.2f}° | "
          f"Min: {row['min_temp']:5.1f}°F, Max: {row['max_temp']:5.1f}°F, "
          f"Mean: {row['mean_temp']:5.1f}°F")

print("\n🏖️  COASTAL ZONES (typical Hawaii weather):")
# Filter for locations with mean temp 70-80°F (typical coastal Hawaii)
coastal = df[(df['mean_temp'] >= 70) & (df['mean_temp'] <= 80)]
print(f"Found {len(coastal)} coastal-type locations")
if len(coastal) > 0:
    print(f"  Temperature range: {coastal['min_temp'].min():.1f}°F to {coastal['max_temp'].max():.1f}°F")
    print(f"  Mean temperature: {coastal['mean_temp'].mean():.1f}°F")

print("\n⛰️  HIGH ELEVATION ZONES (mountains):")
# Filter for locations with mean temp < 60°F (likely mountains)
mountains = df[df['mean_temp'] < 60]
print(f"Found {len(mountains)} high-elevation locations")
if len(mountains) > 0:
    print(f"  Temperature range: {mountains['min_temp'].min():.1f}°F to {mountains['max_temp'].max():.1f}°F")
    print(f"  Mean temperature: {mountains['mean_temp'].mean():.1f}°F")

# Time series analysis
print("\n" + "="*60)
print("📅 TEMPERATURE BY YEAR")
print("="*60)

times = pd.to_datetime(ds['time'].values)
for year in range(2020, 2026):
    year_mask = times.year == year
    if year_mask.sum() > 0:
        year_temps = temp_f[year_mask]
        print(f"{year}: Min={np.nanmin(year_temps):5.1f}°F, "
              f"Max={np.nanmax(year_temps):5.1f}°F, "
              f"Mean={np.nanmean(year_temps):5.1f}°F")

# Seasonal analysis
print("\n" + "="*60)
print("🌤️  TEMPERATURE BY SEASON")
print("="*60)

seasons = {
    'Winter (Dec-Feb)': [12, 1, 2],
    'Spring (Mar-May)': [3, 4, 5],
    'Summer (Jun-Aug)': [6, 7, 8],
    'Fall (Sep-Nov)': [9, 10, 11]
}

for season_name, months in seasons.items():
    season_mask = np.isin(times.month, months)
    if season_mask.sum() > 0:
        season_temps = temp_f[season_mask]
        print(f"{season_name}: Min={np.nanmin(season_temps):5.1f}°F, "
              f"Max={np.nanmax(season_temps):5.1f}°F, "
              f"Mean={np.nanmean(season_temps):5.1f}°F")

print("\n" + "="*60)
print("💡 INTERPRETATION")
print("="*60)
print("✓ If you see temps below 50°F, those are from Mauna Kea/Mauna Loa")
print("✓ Coastal Hawaii temps should be 65-90°F year-round")
print("✓ ERA5 includes ALL elevations in your grid area")
print("✓ Filter by mean temp 70-80°F to focus on coastal areas")