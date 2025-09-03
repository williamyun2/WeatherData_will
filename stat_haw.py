import cdsapi, xarray as xr
import pandas as pd

c = cdsapi.Client("https://cds.climate.copernicus.eu/api", "9a07b105-3cb2-4d69-a6f0-d5c7d8f10d1d")
c.retrieve(
    "reanalysis-era5-single-levels",
    {
        "product_type": "reanalysis",
        "variable": ["geopotential"],   # static orography proxy
        "year": "2024",                 # any year works; field is static
        "month": "01",
        "day": "01",
        "time": "00:00",
        "grid": [0.25, 0.25],           # 0.25° to match your grid
        "area": [23, -161, 18, -154],   # N, W, S, E (lat/lat lon/lon)
        "format": "grib",
    },
    "hawaii_orog.grib",
)

# Load and convert to meters
ds = xr.open_dataset("hawaii_orog.grib", engine="cfgrib")
orog_m = ds["z"] / 9.80665  # 'z' is geopotential in m^2/s^2

# Build a tidy dataframe with only the required columns
df_hi = (
    orog_m.squeeze(drop=True)
          .to_dataframe(name="ElevationMeters")
          .reset_index()
          .rename(columns={"latitude": "Latitude", "longitude": "Longitude"})
)[["Latitude", "Longitude", "ElevationMeters"]]

# Add Region and Country2 as empty
df_hi["Region"] = ""
df_hi["Country2"] = ""

# Save cleaned parquet
df_hi.to_parquet("station_hawaii.parquet", index=False)

print("Saved station_hawaii.parquet with columns:", df_hi.columns.tolist())
