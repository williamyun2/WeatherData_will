import cdsapi, xarray as xr
import pandas as pd

c = cdsapi.Client("https://cds.climate.copernicus.eu/api", "9a07b105-3cb2-4d69-a6f0-d5c7d8f10d1d")
c.retrieve(
    "reanalysis-era5-single-levels",
    {
        "product_type": "reanalysis",
        "variable": ["geopotential"],
        "year": "2024",
        "month": "01", 
        "day": "01",
        "time": "00:00",
        "grid": [0.25, 0.25],
        "area": [23, -161, 18, -154],
        "format": "netcdf",  # Changed from "grib"
    },
    "hawaii_orog.nc",  # Changed extension
)

# # Then open with standard netcdf4 engine
# ds = xr.open_dataset("hawaii_orog.nc", engine="netcdf4")
# orog_m = ds["z"] / 9.80665  # 'z' is geopotential in m^2/s^2

# # Build a tidy dataframe with only the required columns
# df_hi = (
#     orog_m.squeeze(drop=True)
#           .to_dataframe(name="ElevationMeters")
#           .reset_index()
#           .rename(columns={"latitude": "Latitude", "longitude": "Longitude"})
# )[["Latitude", "Longitude", "ElevationMeters"]]

# # Add Region and Country2 as empty
# df_hi["Region"] = ""
# df_hi["Country2"] = ""

# # Save cleaned parquet
# df_hi.to_parquet("station_hawaii.parquet", index=False)

# print("Saved station_hawaii.parquet with columns:", df_hi.columns.tolist())
