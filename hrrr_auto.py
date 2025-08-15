import os
import sys
import struct
import warnings
import concurrent.futures
import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm
from herbie import Herbie
from multiprocessing import cpu_count

warnings.filterwarnings(
    "ignore",
    message="This pattern is interpreted as a regular expression, and has match groups."
)

class HiddenPrints:
    """Context manager to suppress stdout (useful for noisy library calls)."""
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, "w", encoding='utf-8')
    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

def get_single_HRRR(date_, fxx_, product, regex, folder):
    """
    Download and process a single HRRR forecast using Herbie.
    Returns an xarray.Dataset or None if error occurs.
    """
    try:
        with HiddenPrints():
            H = Herbie(
                date_, model="hrrr", product=product, fxx=fxx_, save_dir=folder
            )
            # Always download the full file to ensure index is available
            H.download(verbose=False)
            ds = H.xarray(regex, remove_grib=False)

            def drop_unwanted_coords(ds):
                keep_vars = {"valid_time", "latitude", "longitude"}
                drop_vars = [var for var in ds.coords if var not in keep_vars]
                return ds.drop_vars(drop_vars)

            ds = [drop_unwanted_coords(d) for d in ds]
            ds = xr.merge(ds)
            ds = ds.rename({"x": "lon", "y": "lat"})
            return ds
    except Exception as e:
        print(f"Error in fetching data for {date_} f{fxx_}: {e}")
        return None

def get_multiple_HRRR(dates_, fxxs_, product, regex, folder):
    """
    Download and combine multiple HRRR datasets for a list of dates and forecast hours.
    Returns a sorted, concatenated xarray.Dataset.
    """
    ds_list = []
    if len(dates_) > 1:
        # Historical: multiple dates, same forecast hours
        with concurrent.futures.ProcessPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(get_single_HRRR, date_, fxxs_, product, regex, folder) for date_ in dates_]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    ds_list.append(result)
    else:
        # Forecast: single date, multiple forecast hours
        with concurrent.futures.ProcessPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(get_single_HRRR, dates_[0], fxx_, product, regex, folder) for fxx_ in fxxs_]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result is not None:
                    ds_list.append(result)
    if ds_list:
        ds_merged = xr.concat(ds_list, dim='valid_time')
        return ds_merged.sortby('valid_time')
    else:
        print("No datasets retrieved.")
        return None

def process_day(day):
    """
    Download and process HRRR data for every hour in a day.
    Returns a list of error datetimes.
    """
    errors_dates = []
    ds_list = []
    hours = pd.date_range(day, freq="h", periods=24)
    for h in hours:
        temp = get_single_HRRR(h, [0], "sfc", r":TMP:2 m", "./grib/")
        if temp is not None:
            ds_list.append(temp)
        else:
            errors_dates.append(h)
    if ds_list:
        dss = xr.concat(ds_list, dim='valid_time')
        # dss.to_netcdf(...)  # Save as needed
    return errors_dates

def hrrr_process(ds):
    """
    Process the xarray dataset to convert units and cast data type.
    Returns processed xarray.Dataset.
    """
    ds["sped"] = np.sqrt(ds["u10"] ** 2 + ds["v10"] ** 2)
    ds["sped80"] = np.sqrt(ds["u"] ** 2 + ds["v"] ** 2)
    ds["drct"] = np.arctan2(ds["u10"], ds["v10"])
    ds["t2m"] = np.round((ds["t2m"] - 273.15) * (9 / 5) + 32 + 115)
    ds["d2m"] = np.round((ds["d2m"] - 273.15) * (9 / 5) + 32 + 115)
    ds["sped"] = np.round(ds["sped"] * 2.23694)
    ds["WindSpeed80mph"] = np.round(ds["sped80"] * 2.23694)
    ds["gust"] = np.round(ds["gust"] * 2.23694)
    ds["tcc"] = np.round(ds["tcc"])
    ds["drct"] = np.round(ds["drct"] * 180 / np.pi + 180) / 5
    ds["dswrf"] = ds["sdswrf"] / 5
    ds["dswrf"] = ds["dswrf"].where(ds["dswrf"] >= 0, np.nan)
    ds["prate"] = np.round(ds["prate"] * 3600)
    ds["colmd"] = np.round(40 * np.log10(ds["unknown"] * 1e6))
    ds["colmd"] = ds["colmd"].where(ds["colmd"] > 0, 0)
    ds["cpofp"] = np.round((ds["cpofp"] + 50) * 1 / 1.5)
    ds['cpofp'] = np.round(ds['cpofp'])
    if "t" in ds.variables:
        ds["t"] = ds["t"] / 9.81
        alt = ds["t"].isel(valid_time=0).values.astype(int)
        ds = ds.drop_vars(["t"])
    ds = ds.rename({
        "t2m": "tempF",
        "d2m": "DewPointF",
        "sped": "WindSpeedmph",
        "drct": "WindDirection",
        "tcc": "CloudCoverPerc",
        "dswrf": "GlobalHorizontalIrradianceWM2",
        "gust": "WindGust",
        "prate": "PrecipitationRate",
        "cpofp": "PercentFrozenPrecipitation",
        "colmd": "VerticallyIntegratedSmoke",
    })
    ds_new_columnlist = [
        "tempF", "DewPointF", "WindSpeedmph", "WindDirection",
        "CloudCoverPerc", "WindSpeed80mph", "GlobalHorizontalIrradianceWM2",
        "WindGust", "PrecipitationRate", "PercentFrozenPrecipitation", "VerticallyIntegratedSmoke"
    ]
    ds = ds[ds_new_columnlist]
    ds = ds.where(ds < 255, np.nan)
    ds = ds.transpose("valid_time", "lat", "lon")
    ds = ds.fillna(255)
    ds = ds.astype("uint8")
    return ds



def get_station(ds, state):
    """Get station data for a specific state/region"""
    import numpy as np
    import struct
    from typing import Callable
    
    # Use your existing station_info.npy file
    state_country = np.load("station_info.npy", allow_pickle=True)
    states, countries, elevation = state_country[0], state_country[1], state_country[2]
    
    ds.coords["state"] = (["lat", "lon"], states)
    ds.coords["country"] = (["lat", "lon"], countries)
    ds.coords["elevation"] = (["lat", "lon"], elevation)
    
    df = ds
    if state != "CONUS":  # if the state is not CONUS
        df = ds.where(ds["state"] == state, drop=True)
    
    station_path = f"state_station/{state}_station.pkl"
    
    # Get location data from weather coordinates
    station_sample = df.sel(valid_time=df.valid_time[0])
    station_sample = station_sample.drop_vars([var for var in station_sample.variables][:-6])
    station_sample = station_sample.drop_vars(["valid_time"])
    station = station_sample.to_dataframe()
    station.reset_index(inplace=True)
    station = station[["latitude", "longitude", "elevation", "state", "country"]]
    station.rename(columns={"latitude": "lat", "longitude": "lon"}, inplace=True)
    station["lon"] = station["lon"] - 360

    aMinLat = station.lat.min()
    aMaxLat = station.lat.max()
    aMinLon = station.lon.min()
    aMaxLon = station.lon.max()
    LOC = station.shape[0]
    
    try:
        sta = open(station_path, "rb").read()
    except:
        print(f"Unable to load {state} station data, creating...")
        
        def to_str(x, lens) -> str:
            if (x // 100) == 0:
                return f"{x:.2f}".zfill(lens)
            elif ((-x) // 100) == 0:
                return f"{x:.2f}".zfill(lens + 1)
            else:
                return f"{x:.2f}"

        station["Region"] = station["state"].fillna("").astype(str)
        station["Country2"] = station["country"].fillna("").astype(str)
        station["ElevationMeters"] = station["elevation"].astype(int)
        station["WhoAmI"] = "+" + station["lat"].apply(to_str, args=(5,)) + station["lon"].apply(to_str, args=(6,)) + "/"
        station["WhoAmI"].drop_duplicates(inplace=True)
        
        to_cstring: Callable[[str], str] = lambda s: s.encode("ascii", "replace") + b"\x00"
        station["ascii_null_terminated_WhoAmI"] = station["WhoAmI"].apply(to_cstring)
        station["ascii_null_terminated_Region"] = station["Region"].apply(to_cstring)
        station["ascii_null_terminated_Country2"] = station["Country2"].apply(to_cstring)
        station = station[["ascii_null_terminated_WhoAmI", "ascii_null_terminated_Region", "ascii_null_terminated_Country2", "ElevationMeters", "lat", "lon"]]
        station = station.astype({"lat": "double", "lon": "double", "ElevationMeters": "int16"})
        
        with open(station_path, "wb") as file:
            for row in station.index:
                file.write(struct.pack("<d", station["lat"][row]))
                file.write(struct.pack("<d", station["lon"][row]))
                file.write(struct.pack("<h", station["ElevationMeters"][row]))
                file.write(station["ascii_null_terminated_WhoAmI"][row])
                file.write(station["ascii_null_terminated_Country2"][row])
                file.write(station["ascii_null_terminated_Region"][row])

        sta = open(station_path, "rb").read()
    
    return sta, aMinLat, aMaxLat, aMinLon, aMaxLon, LOC







def NC2PWW(ds, file_path, state="CONUS"):
    """
    Convert the xarray dataset to a PWW format and save it.
    Now supports dynamic station data based on state.
    """
    arr = ds.to_array().values
    arr = arr.transpose(1, 0, 2, 3)
    DATE = (ds.valid_time.values.astype("int64") + 2209161600 * 10**9) / (10**9 * 86400)
    aStartDateTimeUTC = DATE.min()
    aEndDateTimeUTC = DATE.max()
    COUNT = len(DATE)
    
    # Get station data dynamically based on state
    if state == "CONUS":
        # Use the default CONUS station file
        try:
            sta = open("CONUS_station.pkl", "rb").read()
            aMinLat = ds.latitude.values.min()
            aMaxLat = ds.latitude.values.max()
            aMinLon = ds.longitude.values.min() - 360
            aMaxLon = ds.longitude.values.max() - 360
            LOC = ds.latitude.values.flatten().shape[0]
        except:
            # Fallback to dynamic station creation
            sta, aMinLat, aMaxLat, aMinLon, aMaxLon, LOC = get_station(ds, state)
    else:
        # Use dynamic station creation for specific states
        sta, aMinLat, aMaxLat, aMinLon, aMaxLon, LOC = get_station(ds, state)
    
    # Rest of the function remains the same...
    aPWWFileName = file_path
    aPWWVersion = 1
    LOC_FC = 0
    VARCOUNT = arr.shape[1]
    
    with open(aPWWFileName, "wb") as file:
        file.write(struct.pack("<h", 2001))
        file.write(struct.pack("<h", 8065))
        file.write(struct.pack("<h", aPWWVersion))
        file.write(struct.pack("<d", aStartDateTimeUTC))
        file.write(struct.pack("<d", aEndDateTimeUTC))
        file.write(struct.pack("<d", aMinLat))
        file.write(struct.pack("<d", aMaxLat))
        file.write(struct.pack("<d", aMinLon))
        file.write(struct.pack("<d", aMaxLon))
        file.write(struct.pack("<h", 0))
        file.write(struct.pack("<i", COUNT))
        file.write(struct.pack("<i", 3600))
        file.write(struct.pack("<i", LOC))
        file.write(struct.pack("<h", LOC_FC))
        file.write(struct.pack("<h", VARCOUNT))
        # Variable codes...
        file.write(struct.pack("<h", 102))  # tempF
        file.write(struct.pack("<h", 104))  # DewPointF
        file.write(struct.pack("<h", 106))  # WindSpeedmph
        file.write(struct.pack("<h", 107))  # WindDirection
        file.write(struct.pack("<h", 119))  # CloudCoverPerc
        file.write(struct.pack("<h", 112))  # WindSpeed80mph
        file.write(struct.pack('<h', 120))  # GlobalHorizontalIrradianceWM2
        file.write(struct.pack("<h", 136))  # WindGust
        file.write(struct.pack("<h", 151))  # PrecipitationRate
        file.write(struct.pack("<h", 150))  # PercentFrozenPrecipitation
        file.write(struct.pack("<h", 122))  # VerticallyIntegratedSmoke
        file.write(struct.pack("<h", arr.shape[1]))  # BYTECOUNT
        file.write(sta)
        file.write(arr.tobytes())




        
if __name__ == "__main__":
    # Example usage: fetch one day of data
    errors = process_day(pd.Timestamp("2025-05-01"))
    if errors:
        print("Errors occurred for the following datetimes:", errors)
