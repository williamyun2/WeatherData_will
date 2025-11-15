# author:  T-T 


import cdsapi
import time  # Use this instead of 'from datetime import time'
from datetime import datetime, timedelta  # Import specific datetime classes
import os, sys, shutil
from tqdm import tqdm
import pandas as pd
import xarray as xr
import numpy as np
import zipfile
import struct, re
from glob import glob

import logging
import logging.config
import logging.handlers

# import cronitor

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive


# * Set the timezone to CDT https://www.geeksforgeeks.org/python-time-tzset-function/
if os.name != "nt":
    os.environ["TZ"] = "America/Chicago"
    time.tzset()


# cronitor.api_key = "6a728d6edcea4ec885f014fa1aa76dd9" william's api key, not used though
CDS = cdsapi.Client("https://cds.climate.copernicus.eu/api", "9a07b105-3cb2-4d69-a6f0-d5c7d8f10d1d")

# * Set the path to the data folder
sys.path.append(os.path.dirname(__file__))
os.makedirs(os.path.join(os.path.dirname(__file__), "data"), exist_ok=True)
Data = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")  # * Path to the data folder

# Add parent directory to the module search path(only need for debugging)
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)

from helper import helper

# print(sys.path)
# print(os.getcwd())


# set the logging configuration for the script
DEBUG = False
log_file = f"{Data}/download.log"
logging.basicConfig(filename=log_file, level=logging.DEBUG if DEBUG else logging.INFO, format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M")
logger = logging.getLogger("ERA_HISTORY")

# Reduce verbosity
logging.getLogger('cdsapi').setLevel(logging.ERROR)  # Changed from WARNING to ERROR
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('cdsapi.api').setLevel(logging.ERROR)  # Add this line



def fetch_data(stime, etime, file_path, area=None):
    if area is None:
        raise ValueError("Area parameter is required. Please provide coordinates as a list: [North, West, South, East]\n"
                        "Example: area=['23', '-161', '18', '-154'] for Hawaii\n"
                        "Or use predefined areas: HAWAII, CONUS, TEXAS, CALIFORNIA \n \n")
    
    CDS.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "format": "netcdf",
            "download_format": "zip",
            "variable": [
                "2m_dewpoint_temperature",
                "2m_temperature",
                "100m_u_component_of_wind",
                "100m_v_component_of_wind",
                "10m_u_component_of_wind",
                "10m_v_component_of_wind",
                "total_cloud_cover",
                "high_cloud_cover",
                "low_cloud_cover",
                "medium_cloud_cover",
                "surface_solar_radiation_downwards",
                "total_sky_direct_solar_radiation_at_surface",
                "geopotential",
                "10m_wind_gust_since_previous_post_processing",
            ],
            "area": area,  # Use the passed area
            "time": pd.date_range(stime, etime+timedelta(1), freq="h", normalize=True).strftime("%H:%M").unique().tolist(),
            "day": pd.date_range(stime, etime, freq="h", inclusive="both").strftime("%d").unique().tolist(),
            "month": pd.date_range(stime, etime, freq="h").strftime("%m").unique().tolist(),
            "year": pd.date_range(stime, etime, freq="h").strftime("%Y").unique().tolist(),
        },
        file_path,
    )


def zip_to_nc(zip_file_path, nc_path):
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(nc_path)
        # remove the zip file after extracting
        # os.remove(zip_file_path)
    ds_acc = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-accum.nc")
    ds = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-instant.nc")
    ds_gust = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-max.nc")
    return xr.merge([ds, ds_acc, ds_gust])


def GetMeta(path):  # get the meta file, if not exist create one
    try:
        meta = pd.read_csv(f"{path}/meta.csv")
    except:
        meta = pd.DataFrame(columns=["date", "status"])
    return meta


def NCtoPWW(df, nc_path):

    def to_str(x, lens) -> str:
        if (x // 100) == 0:
            return f"{x:.2f}".zfill(lens)
        elif ((-x) // 100) == 0:
            return f"{x:.2f}".zfill(lens + 1)
        else:
            return f"{x:.2f}"

    df["sped"] = np.sqrt(df["u10"] ** 2 + df["v10"] ** 2)
    df["sped100"] = np.sqrt(df["u100"] ** 2 + df["v100"] ** 2)
    df["drct"] = np.arctan2(df["u10"], df["v10"])
    df["tcc"].where(df["tcc"].isnull(), (df["hcc"] + df["mcc"] + df["lcc"]) / 3)
    # ......... UNIT CONVERSION
    #!!! reduce the bit size of the data by dividing by 5 or add offset of 115
    df["t2m"] = np.round((df["t2m"] - 273.15) * 9 / 5 + 32 + 115)  # convert to degF with 115 offset
    df["d2m"] = np.round((df["d2m"] - 273.15) * 9 / 5 + 32 + 115)  # convert to degF
    df["sped"] = np.round(df["sped"] * 2.236936)  # convert from M/s => mph
    df["WindSpeed100mph"] = np.round(df["sped100"] * 2.23694)  # convert from mps to mph
    df["fg10"] = np.round(df["fg10"] * 2.236936)  # convert wind gust from M/s => mph
    df["tcc"] = np.round(df["tcc"] * 100)  # convert to %
    df["drct"] = np.round(df["drct"] * 180 / np.pi + 180) / 5  # convert to deg
    df["ssrd"] = df["ssrd"] / (3600 * 5)  # J/m^2 => W/m^2
    df["fdir"] = df["fdir"] / (3600 * 5)  # J/m^2 => W/m^2

    df = df.sortby(["valid_time", "latitude", "longitude"])
    df = df.rename(
        {
            "t2m": "tempF",
            "d2m": "DewPointF",
            "sped": "WindSpeedmph",
            "drct": "WindDirection",
            "tcc": "CloudCoverPerc",
            "ssrd": "GlobalHorizontalIrradianceWM2",
            "fdir": "DirectHorizontalIrradianceWM2",
            "fg10": "GustSpeedmph",
        }
    )
    df = df.transpose("valid_time", "latitude", "longitude")
    df_new_columnlist = [
        "tempF",
        "DewPointF",
        "WindSpeedmph",
        "WindDirection",
        "CloudCoverPerc",
        "WindSpeed100mph",
        "GlobalHorizontalIrradianceWM2",
        "DirectHorizontalIrradianceWM2",
        "GustSpeedmph",
    ]
    df = df[df_new_columnlist]
    df = df.where(df < 255, np.nan)  #! try to limit overflow result of incorrect casting
    df = df.fillna(255)
    df = df.astype("uint8")
    arr = df.to_array().values
    print(f"Shape: {arr.shape}")
    arr = arr.transpose(1, 0, 2, 3)
    print(f"trans Shape: {arr.shape}")

    # * get the station data
    print(os.getcwd())
    station = pd.read_parquet("station/hawaii_station.parquet")  # station.parquet
    aMinLat = station.Latitude.min()
    aMaxLat = station.Latitude.max()
    aMinLon = station.Longitude.min()
    aMaxLon = station.Longitude.max()
    LOC = station.shape[0]

    # * covert the data to excel format
    DATE = (df.valid_time.astype("int64") + 2209161600 * 10**9) / (10**9 * 86400)  # convert to days since 1970-01-01
    aStartDateTimeUTC = DATE.min()
    aEndDateTimeUTC = DATE.max()
    COUNT = len(DATE)

    # * create the pww file
    aPWWVersion = 1
    LOC_FC: int = 0  # for extra loc variables from table 1
    VARCOUNT: int = arr.shape[1]  # Set this to the number of weather variable types you have
    sta = open("station/hawaii_era5_station.pkl", "rb").read()

    with open(nc_path, "wb") as file:
        # ......... FILE HEADER .........#
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
        file.write(struct.pack("<i", COUNT))  # countNumber of datetime values (COUNT)
        file.write(struct.pack("<i", 3600))
        file.write(struct.pack("<i", LOC))  # Number of weather measurement locations (LOC)
        file.write(struct.pack("<h", LOC_FC))  # Loc_FC # Pack the data into INT16 format and write to stream
        file.write(struct.pack("<h", VARCOUNT))
        file.write(struct.pack("<h", 102))  # Temp in F
        file.write(struct.pack("<h", 104))  # Dew point in F
        file.write(struct.pack("<h", 106))  # Wind speed at surface (10m) in mph
        file.write(struct.pack("<h", 107))  # Wind direction at surface (10m) in 5-degree increments
        file.write(struct.pack("<h", 119))  # Total cloud cover percentage
        file.write(struct.pack("<h", 110))  # Wind speed at 100m in mph
        file.write(struct.pack("<h", 120))  # Global Horizontal Irradiance in W/m^2 divided by 4
        file.write(struct.pack("<h", 121))  # Direct Horizontal Irradiance in W/m^2 divided by 4
        file.write(struct.pack("<h", 136))  # Wind Gust at 10m in mph
        # ......... STATION DATA .........#
        file.write(struct.pack("<h", arr.shape[1]))  # BYTECOUNT
        file.write(sta)  # Write the station data
        # ......... DATA .........#
        file.write(arr.tobytes())

































def pack_quarter(target_date=None):
    if target_date is None:
        today = datetime.now() - timedelta(days=7)
    else:
        today = target_date
    
    quarter = (today.month - 1) // 3 + 1


    print(f"Current Quarter: {quarter}")
    quarter_lookup = {  # not sure if there is a better way to do this, this is robust for leap year
        1: [datetime(today.year, 1, 1), datetime(today.year, 3, 31)],
        2: [datetime(today.year, 4, 1), datetime(today.year, 6, 30)],
        3: [datetime(today.year, 7, 1), datetime(today.year, 9, 30)],
        4: [datetime(today.year, 10, 1), datetime(today.year, 12, 31)],

    }

    quarter_start, quarter_end = quarter_lookup[quarter]
    logger.info(f"getting {quarter}, Quarter Start: {quarter_start}, Quarter End: {quarter_end}")

    date_pattern = re.compile(r"(\d{8})")  # defualt pattern for the nc folder name

    # Get the list of files in the directory
    files = glob(rf"{Data}/nc/*")

    # Filter the files using the regex pattern and date range
    matched_files = []
    for file in files:
        match = date_pattern.search(file)
        if match:
            stime = datetime.strptime(match.group(1), "%Y%m%d")
            # etime = datetime.strptime(match.group(2), "%Y%m%d")
            if quarter_start <= stime <= quarter_end:  # and quarter_start <= etime <= quarter_end:
                matched_files.append(file)
    logger.info(f"found {len(files)} files in the directory,and {len(matched_files)} files in the {quarter}quarter")

    # Merge the files, due to the stupid way the nc was spread into 3 files
    ncs = []
    actual_dates = []
    for nc_path in matched_files:
        ds_acc = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-accum.nc")
        ds = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-instant.nc")
        ds_gust = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-max.nc")
        ncs.append(xr.merge([ds, ds_acc, ds_gust]))
        # Extract actual date from filename for date range naming
        match = date_pattern.search(nc_path)
        if match:
            actual_dates.append(datetime.strptime(match.group(1), "%Y%m%d"))
    
    ds = xr.concat(ncs, dim="valid_time")
    ds = ds.dropna("valid_time", how="all")
    ds = ds.drop_duplicates("valid_time")
    
    # Create filename based on actual date range of available data
    if actual_dates:
        actual_start = min(actual_dates)
        actual_end = max(actual_dates)
        if actual_start == actual_end:
            # Single date - only show date once
            file_name = f"Hawaii_{actual_start.strftime('%Y-%m-%d')}"
        else:
            # Date range - show start to end
            file_name = f"Hawaii_{actual_start.strftime('%Y-%m-%d')}_to_{actual_end.strftime('%Y-%m-%d')}"
    else:
        # Fallback to quarter naming if no files found
        file_name = f"Hawaii{quarter_start.year}_Q{quarter}"
    
    return ds, file_name


























































































def get_date_range(start_date=None, end_date=None):
    # Validate date types
    if start_date is not None and not isinstance(start_date, datetime):
        raise TypeError(f"start_date must be a datetime object, got {type(start_date)}.\n"
                       "Example: datetime(2025, 8, 10)")
    
    if end_date is not None and not isinstance(end_date, datetime):
        raise TypeError(f"\n \n end_date must be a datetime object, got {type(end_date)}.\n"
                       "Example: datetime(2025, 8, 15)")
    
    # Validate date logic
    if start_date is not None and end_date is not None and end_date < start_date:
        raise ValueError(f"\n \n end_date ({end_date.strftime('%Y-%m-%d')}) cannot be before start_date ({start_date.strftime('%Y-%m-%d')}) \n \n ")
    
    if start_date is None:
        # Default behavior - last 2 weeks
        today = datetime.now()
        current_date = today - timedelta(days=5)
        past_date = current_date - timedelta(weeks=2)
        dates = pd.date_range(past_date, current_date, freq="D", inclusive="both", normalize=True)
    elif end_date is None:
        # Single date
        dates = pd.date_range(start_date, start_date, freq="D", normalize=True)
    else:
        # Date range
        dates = pd.date_range(start_date, end_date, freq="D", inclusive="both", normalize=True)
    
    return dates

def main(start_date=None, end_date=None, area=None):
    # * Check if the data folder exists, if not create it
    check = lambda p: os.makedirs(p, exist_ok=True)
    check(f"{Data}/nc/")
    check(f"{Data}/pww/quarter/")
    check(f"{Data}/pww/daily/")
    check(f"{Data}/zip/")

    # * Get date range based on parameters
    dates = get_date_range(start_date, end_date)
    
    print(f"Fetching data from {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")

    # ******************** CORRECTED SECTION START ********************
    # * Setup Google Drive authentication using the service account
    # Load settings from the YAML file to specify service account credentials
    settings_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.yaml")
    gauth = GoogleAuth(settings_file=settings_file)
    # Authenticate using the service account configuration from settings.yaml
    gauth.ServiceAuth() # This method takes no arguments
    drive = GoogleDrive(gauth)
    # ********************* CORRECTED SECTION END *********************

    # * import the helper functions
    hp = helper(logger)
    # * get the meta file to check the missing data
    meta = GetMeta(Data)
    meta["date"] = pd.to_datetime(meta["date"])
    meta["status"] = meta["status"].astype(bool)
    dates = dates[~dates.isin(meta.loc[meta["status"] == 1, "date"])]  # remove the dates that have been fetched and without missing data(status==1)
    print(f"founds {len(dates)} dates to be fetched")
    # * fetch the data and process it check if the data is missing
    date_ = []  # store the date for meta file
    status_ = []  # store the status for meta file

# To this (for example)
    for d in tqdm(dates):
        try:  # * fetch the data---> convert the zip to nc---> convert the nc to pww
            processing_date = d.date() # <--- Renamed variable
            day = f"{processing_date.day:02d}"
            month = f"{processing_date.month:02d}"
            year = f"{processing_date.year}"
            file_name = f"{year}{month}{day}"
            
            # Check if data already exists locally
            nc_folder_path = f"{Data}/nc/{file_name}"
            zip_file_path = f"{Data}/zip/{file_name}.zip"
            
            if os.path.exists(nc_folder_path) and os.path.exists(zip_file_path):
                # Data already exists, just process it
                # print(f"Data for {processing_date} already exists locally, processing...")
                ds = zip_to_nc(zip_file_path, nc_folder_path)
            else:
                # Need to download data
                # print(f"Downloading data for {processing_date}...")
                fetch_data(processing_date, processing_date, zip_file_path, area=area)  # Pass area here
                check(nc_folder_path)
                ds = zip_to_nc(zip_file_path, nc_folder_path)
            
            if ds.isnull().to_array().sum().values > 0:
                # Update meta immediately for missing data
                meta = pd.concat([meta, pd.DataFrame({"date": [d], "status": [False]})], ignore_index=True)
                meta = meta.drop_duplicates(subset="date", keep="last")
                meta.to_csv(f"{Data}/meta.csv", index=False)
                print(f"Data for {processing_date} have {ds.isnull().sum().sum()} missing data")
            # --- Change it to this ---
            else:
                NCtoPWW(ds, f"{Data}/pww/daily/{file_name}.pww")  # Process the data first
                # Update meta immediately for successful processing
                meta = pd.concat([meta, pd.DataFrame({"date": [d], "status": [True]})], ignore_index=True)
                meta = meta.drop_duplicates(subset="date", keep="last")
                meta.to_csv(f"{Data}/meta.csv", index=False)
                # print(f"Data for {processing_date} have been successfully processed")
        except Exception as e:
            # Update meta immediately for failed processing
            meta = pd.concat([meta, pd.DataFrame({"date": [d], "status": [False]})], ignore_index=True)
            meta = meta.drop_duplicates(subset="date", keep="last")
            meta.to_csv(f"{Data}/meta.csv", index=False)
            print(f"\n \n \n Error in process data for {processing_date}, {e}") # <--- Update here as well

    # * pack the data into each quarter
    #! need to consder the date the file is mssing after packing
    # ds, file_name = pack_quarter()
    # ds, file_name = pack_quarter(datetime(2023, 7, 15))  # Any date in July 2023
    if start_date and end_date:
        # Use the middle date of the range for quarter determination
        quarter_date = start_date + (end_date - start_date) / 2
    elif start_date:
        quarter_date = start_date
    else:
        quarter_date = None
        
    ds, file_name = pack_quarter(quarter_date)
    # print(ds)
    NCtoPWW(ds, f"{Data}/pww/quarter/{file_name}.pww")

    # team overbye google drive
    # daily_folder_id = "1jN1NP3b5Nby-gpy5w1rqe2cgctESxqO-"
    # daily_archive_folder_id = "1QkSwW9eLtBjo0Q5ia8akZkMpqJuMivDp"
    # quarterly_folder_id = "12U8PNHHGIxCy8_GRzsF2KxZ4GneMWy6h"

        
    # test folders cds 
    # daily_folder_id = "1dmXrU8qtkMkPbQl6QxNToZBUmjIORIxe"
    # daily_archive_folder_id = "1EepB8GlTLqOl5iSgXz0WEINw6lcjyuaa"
    # quarterly_folder_id = "1h4TeCcAc0khTkeGFtSNubwgFsY5CD8pH"

    # hawaii
    # daily_folder_id = "10CBuzq1RwiswXkV7T_cVWCjiLhCaKngF"
    # daily_archive_folder_id = "10CBuzq1RwiswXkV7T_cVWCjiLhCaKngF"
    # quarterly_folder_id = "10CBuzq1RwiswXkV7T_cVWCjiLhCaKngF"

    # Before archiving, upload only truly new files
    # logger.info("Uploading daily .pww files to the cloud")
    # hp.upload_to_drive(drive, daily_folder_id, f"{Data}/pww/daily/*.pww", overwrite=False,
    #     archive_folder_id=daily_archive_folder_id  # Add this!
    #     )

    # logger.info("Archiving old files from the daily folder")
    # hp.archive_folder(drive, daily_folder_id, daily_archive_folder_id, timedelta(weeks=2))

    # logger.info(f"Uploading {file_name}.pww to the cloud")
    # # Upload the quarterly data, overwriting any existing file with the same name
    # hp.upload_to_drive(drive, quarterly_folder_id, f"{Data}/pww/quarter/*.pww", overwrite=True)









# Now you can specify coordinates right here at the bottom!
if __name__ == "__main__":
    # Common coordinate sets for easy copy/paste:
    HAWAII = ["23", "-161", "18", "-154"]
    CONUS = ["58", "-130", "24", "-60"]  # Continental US
    TEXAS = ["37", "-107", "25", "-93"]
    CALIFORNIA = ["42", "-125", "32", "-114"]
    
    # Examples of how to call:
    
   # Option 1: Default behavior (Hawaii, last 2 weeks)  
    # main(area=HAWAII)

    # Option 2: Single date with custom area
    # main(start_date=datetime(2023, 7, 15), area=TEXAS)

    # Option 3: Date range with custom coordinates
    main(start_date=datetime(2025, 8, 10), end_date=datetime(2025, 8, 15), area=HAWAII)
    
    # Option 4: Custom coordinates 
    # main(start_date=datetime(2025, 8, 10), end_date=datetime(2025, 9, 6), 
    #      area=["30", "-100", "25", "-95"])  # CUSTOM AREA, CUSTOM AREA, CUSTOM AREA, CUSTOM AREA, CUSTOM AREA, CUSTOM AREA, CUSTOM AREA, 