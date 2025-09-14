# Enhanced cds_history.py with automatic station data generation
# Just change the area coordinates at the bottom and run this single file!

import cdsapi
import time
from datetime import datetime, timedelta
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

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# Set the timezone to CDT
if os.name != "nt":
    os.environ["TZ"] = "America/Chicago"
    time.tzset()

CDS = cdsapi.Client("https://cds.climate.copernicus.eu/api", "9a07b105-3cb2-4d69-a6f0-d5c7d8f10d1d")

# Set the path to the data folder
sys.path.append(os.path.dirname(__file__))
os.makedirs(os.path.join(os.path.dirname(__file__), "data"), exist_ok=True)
Data = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs("station", exist_ok=True)

# Add parent directory to the module search path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)

try:
    from helper import helper
except ImportError:
    print("Warning: helper module not found, continuing without it")
    helper = None

# Set up logging
DEBUG = False
log_file = f"{Data}/download.log"
logging.basicConfig(filename=log_file, level=logging.DEBUG if DEBUG else logging.INFO, 
                   format="{asctime} - {levelname} - {message}", style="{", datefmt="%Y-%m-%d %H:%M")
logger = logging.getLogger("ERA_HISTORY")

# Reduce verbosity
logging.getLogger('cdsapi').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('cdsapi.api').setLevel(logging.ERROR)

def to_str(x, lens) -> str:
    """Convert coordinate to string with proper padding"""
    if (x // 100) == 0:
        return f"{x:.2f}".zfill(lens)
    elif ((-x) // 100) == 0:
        return f"{x:.2f}".zfill(lens + 1)
    else:
        return f"{x:.2f}"

def generate_station_data(area, region_name="region"):
    """
    Automatically generate station data for the specified area
    area: [North, West, South, East] coordinates
    region_name: string identifier for the region
    """
    print(f"🗺️  Generating station data for {region_name}...")
    
    # Step 1: Download reference geopotential data
    print("📥 Downloading geopotential data...")
    nc_file = f"{region_name}_orog.nc"
    
    CDS.retrieve(
        "reanalysis-era5-single-levels",
        {
            "product_type": "reanalysis",
            "variable": ["geopotential"],
            "year": "2024",
            "month": "01", 
            "day": "01",
            "time": "00:00",
            "grid": [0.25, 0.25],
            "area": area,  # [North, West, South, East]
            "format": "netcdf",
        },
        nc_file,
    )
    
    # Step 2: Convert NC to station parquet
    print("🔄 Converting to station data...")
    with xr.open_dataset(nc_file, engine="netcdf4") as ds:
        df = ds.to_dataframe().reset_index()
    df.reset_index(inplace=True)
    
    # Create station identifiers
    df['station_id'] = df['longitude'].apply(to_str, args=(6,)) + '_' + df['latitude'].apply(to_str, args=(5,))
    df['station_id'].drop_duplicates(inplace=True)
    df['Country2'] = ''
    df['Region'] = ''
    df['WMO'] = 0
    df['ElevationMeters'] = df['z'] / 9.80665  # Convert geopotential to elevation
    df['ICAO'] = ''
    
    # Clean up dataframe
    df = df[['station_id', 'longitude', 'latitude', 'ElevationMeters', 'ICAO', 'WMO', 'Region', 'Country2']]
    df.rename(columns={"station_id": "Name", 'longitude': 'Longitude', 'latitude': 'Latitude'}, inplace=True)
    df.set_index('Name', inplace=True)
    
    # Save station data
    station_parquet = f"station/{region_name}_station.parquet"
    df.to_parquet(station_parquet, index=False)
    print(f"✅ Saved {station_parquet}")
    
    # Step 3: Generate PKL file
    print("📦 Generating binary station file...")
    generate_station_pkl(station_parquet, region_name)
    
    # Cleanup
    if os.path.exists(nc_file):
        os.remove(nc_file)
    
    return station_parquet

def generate_station_pkl(parquet_file, region_name):
    """Generate the binary PKL station file"""
    from typing import Callable
    
    # Load station data
    station = pd.read_parquet(parquet_file)
    
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
    pkl_file = f"station/{region_name}_era5_station.pkl"
    with open(pkl_file, "wb") as file:
        for row in station.index:
            file.write(struct.pack('<d', station['Latitude'][row]))          # Write Latitude (DOUBLE)
            file.write(struct.pack('<d', station['Longitude'][row]))         # Write Longitude (DOUBLE)
            file.write(struct.pack('<h', station['ElevationMeters'][row]))   # Write AltitudeM (INT16)
            file.write(station['ascii_null_terminated_WhoAmI'][row])         # Write Name (CSTRING)
            file.write(station['ascii_null_terminated_Country2'][row])       # Write Country (CSTRING)
            file.write(station['ascii_null_terminated_Region'][row])         # Write Region (CSTRING)
    
    print(f"✅ Created {pkl_file} with {len(station)} stations")
    print("First few stations:")
    print(station[['Latitude', 'Longitude', 'ElevationMeters', 'WhoAmI']].head())

def fetch_data(stime, etime, file_path, area=None):
    if area is None:
        raise ValueError("Area parameter is required. Please provide coordinates as a list: [North, West, South, East]\n"
                        "Example: area=['23', '-161', '18', '-154'] for Hawaii\n"
                        "Or use predefined areas: HAWAII, CONUS, TEXAS, CALIFORNIA \n \n")
    if os.path.exists(file_path):
        print(f"✅ Using existing file {file_path}, skipping download")
        return

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
            "area": area,
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
    ds_acc = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-accum.nc")
    ds = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-instant.nc")
    ds_gust = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-max.nc")
    return xr.merge([ds, ds_acc, ds_gust])

def GetMeta(path):
    try:
        meta = pd.read_csv(f"{path}/meta.csv")
    except:
        meta = pd.DataFrame(columns=["date", "status"])
    return meta

def NCtoPWW(df, nc_path, region_name):
    """Convert NetCDF data to PWW format using the specified region's station data"""
    
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
    
    # Unit conversions
    df["t2m"] = np.round((df["t2m"] - 273.15) * 9 / 5 + 32 + 115)
    df["d2m"] = np.round((df["d2m"] - 273.15) * 9 / 5 + 32 + 115)
    df["sped"] = np.round(df["sped"] * 2.236936)
    df["WindSpeed100mph"] = np.round(df["sped100"] * 2.23694)
    df["fg10"] = np.round(df["fg10"] * 2.236936)
    df["tcc"] = np.round(df["tcc"] * 100)
    df["drct"] = np.round(df["drct"] * 180 / np.pi + 180) / 5
    df["ssrd"] = df["ssrd"] / (3600 * 5)
    df["fdir"] = df["fdir"] / (3600 * 5)

    df = df.sortby(["valid_time", "latitude", "longitude"])
    df = df.rename({
        "t2m": "tempF",
        "d2m": "DewPointF",
        "sped": "WindSpeedmph",
        "drct": "WindDirection",
        "tcc": "CloudCoverPerc",
        "ssrd": "GlobalHorizontalIrradianceWM2",
        "fdir": "DirectHorizontalIrradianceWM2",
        "fg10": "GustSpeedmph",
    })
    
    df = df.transpose("valid_time", "latitude", "longitude")
    df_new_columnlist = [
        "tempF", "DewPointF", "WindSpeedmph", "WindDirection", "CloudCoverPerc",
        "WindSpeed100mph", "GlobalHorizontalIrradianceWM2", "DirectHorizontalIrradianceWM2", "GustSpeedmph",
    ]
    df = df[df_new_columnlist]
    df = df.where(df < 255, np.nan)
    df = df.fillna(255)
    df = df.astype("uint8")
    arr = df.to_array().values
    print(f"Shape: {arr.shape}")
    arr = arr.transpose(1, 0, 2, 3)
    print(f"trans Shape: {arr.shape}")

    # Load the appropriate station data
    station_parquet = f"station/{region_name}_station.parquet"
    if not os.path.exists(station_parquet):
        raise FileNotFoundError(f"Station data not found: {station_parquet}")
    
    station = pd.read_parquet(station_parquet)
    aMinLat = station.Latitude.min()
    aMaxLat = station.Latitude.max()
    aMinLon = station.Longitude.min()
    aMaxLon = station.Longitude.max()
    LOC = station.shape[0]

    # Convert the data to excel format
    DATE = (df.valid_time.astype("int64") + 2209161600 * 10**9) / (10**9 * 86400)
    aStartDateTimeUTC = DATE.min()
    aEndDateTimeUTC = DATE.max()
    COUNT = len(DATE)

    # Create the pww file
    aPWWVersion = 1
    LOC_FC: int = 0
    VARCOUNT: int = arr.shape[1]
    
    # Load the binary station data
    pkl_file = f"station/{region_name}_era5_station.pkl"
    if not os.path.exists(pkl_file):
        raise FileNotFoundError(f"Binary station data not found: {pkl_file}")
    
    sta = open(pkl_file, "rb").read()

    with open(nc_path, "wb") as file:
        # FILE HEADER
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
        file.write(struct.pack("<h", 102))  # Temp in F
        file.write(struct.pack("<h", 104))  # Dew point in F
        file.write(struct.pack("<h", 106))  # Wind speed at surface (10m) in mph
        file.write(struct.pack("<h", 107))  # Wind direction at surface (10m) in 5-degree increments
        file.write(struct.pack("<h", 119))  # Total cloud cover percentage
        file.write(struct.pack("<h", 110))  # Wind speed at 100m in mph
        file.write(struct.pack("<h", 120))  # Global Horizontal Irradiance in W/m^2 divided by 4
        file.write(struct.pack("<h", 121))  # Direct Horizontal Irradiance in W/m^2 divided by 4
        file.write(struct.pack("<h", 136))  # Wind Gust at 10m in mph
        
        # STATION DATA
        file.write(struct.pack("<h", arr.shape[1]))
        file.write(sta)
        
        # DATA
        file.write(arr.tobytes())

def pack_quarter(target_date=None):
    # [Keep the existing pack_quarter function unchanged]
    if target_date is None:
        today = datetime.now() - timedelta(days=7)
    else:
        today = target_date
    
    quarter = (today.month - 1) // 3 + 1
    print(f"Current Quarter: {quarter}")
    quarter_lookup = {
        1: [datetime(today.year, 1, 1), datetime(today.year, 3, 31)],
        2: [datetime(today.year, 4, 1), datetime(today.year, 6, 30)],
        3: [datetime(today.year, 7, 1), datetime(today.year, 9, 30)],
        4: [datetime(today.year, 10, 1), datetime(today.year, 12, 31)],
    }

    quarter_start, quarter_end = quarter_lookup[quarter]
    logger.info(f"getting {quarter}, Quarter Start: {quarter_start}, Quarter End: {quarter_end}")

    date_pattern = re.compile(r"(\d{8})")
    files = glob(rf"{Data}/nc/*")
    matched_files = []
    for file in files:
        match = date_pattern.search(file)
        if match:
            stime = datetime.strptime(match.group(1), "%Y%m%d")
            if quarter_start <= stime <= quarter_end:
                matched_files.append(file)
    logger.info(f"found {len(files)} files in the directory,and {len(matched_files)} files in the {quarter}quarter")

    ncs = []
    actual_dates = []
    for nc_path in matched_files:
        ds_acc = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-accum.nc")
        ds = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-instant.nc")
        ds_gust = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-max.nc")
        ncs.append(xr.merge([ds, ds_acc, ds_gust]))
        match = date_pattern.search(nc_path)
        if match:
            actual_dates.append(datetime.strptime(match.group(1), "%Y%m%d"))
    
    ds = xr.concat(ncs, dim="valid_time")
    ds = ds.dropna("valid_time", how="all")
    ds = ds.drop_duplicates("valid_time")
    
    if actual_dates:
        actual_start = min(actual_dates)
        actual_end = max(actual_dates)
        if actual_start == actual_end:
            file_name = f"Hawaii_{actual_start.strftime('%Y-%m-%d')}"
        else:
            file_name = f"Hawaii_{actual_start.strftime('%Y-%m-%d')}_to_{actual_end.strftime('%Y-%m-%d')}"
    else:
        file_name = f"Hawaii{quarter_start.year}_Q{quarter}"
    
    return ds, file_name

def get_date_range(start_date=None, end_date=None):
    # [Keep the existing get_date_range function unchanged]
    if start_date is not None and not isinstance(start_date, datetime):
        raise TypeError(f"start_date must be a datetime object, got {type(start_date)}.\n"
                       "Example: datetime(2025, 8, 10)")
    
    if end_date is not None and not isinstance(end_date, datetime):
        raise TypeError(f"\n \n end_date must be a datetime object, got {type(end_date)}.\n"
                       "Example: datetime(2025, 8, 15)")
    
    if start_date is not None and end_date is not None and end_date < start_date:
        raise ValueError(f"\n \n end_date ({end_date.strftime('%Y-%m-%d')}) cannot be before start_date ({start_date.strftime('%Y-%m-%d')}) \n \n ")
    
    if start_date is None:
        today = datetime.now()
        current_date = today - timedelta(days=5)
        past_date = current_date - timedelta(weeks=2)
        dates = pd.date_range(past_date, current_date, freq="D", inclusive="both", normalize=True)
    elif end_date is None:
        dates = pd.date_range(start_date, start_date, freq="D", normalize=True)
    else:
        dates = pd.date_range(start_date, end_date, freq="D", inclusive="both", normalize=True)
    
    return dates

def main(start_date=None, end_date=None, area=None, region_name="region"):
    """
    Main function with automatic station data generation
    
    Args:
        start_date: Start date for data download
        end_date: End date for data download  
        area: [North, West, South, East] coordinates
        region_name: Name for the region (used in file naming)
    """
    
    if area is None:
        raise ValueError("Area parameter is required!")
    
    print(f"🚀 Starting automated pipeline for {region_name}")
    print(f"📍 Area: {area}")
    
    # Check if station data exists, if not generate it
    station_parquet = f"station/{region_name}_station.parquet"
    station_pkl = f"station/{region_name}_era5_station.pkl"
    
    if not os.path.exists(station_parquet) or not os.path.exists(station_pkl):
        print(f"🔧 Station data not found, generating automatically...")
        generate_station_data(area, region_name)
    else:
        print(f"✅ Station data found: {station_parquet}")
    
    # Create necessary directories
    check = lambda p: os.makedirs(p, exist_ok=True)
    check(f"{Data}/nc/")
    check(f"{Data}/pww/quarter/")
    check(f"{Data}/pww/daily/")
    check(f"{Data}/zip/")

    # Get date range
    dates = get_date_range(start_date, end_date)
    print(f"📅 Fetching data from {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")

    # Setup Google Drive (optional)
    try:
        settings_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.yaml")
        if os.path.exists(settings_file):
            gauth = GoogleAuth(settings_file=settings_file)
            gauth.ServiceAuth()
            drive = GoogleDrive(gauth)
        else:
            drive = None
    except:
        drive = None
        print("⚠️  Google Drive setup failed, continuing without cloud upload")

    # Import helper if available
    if helper:
        hp = helper(logger)
    else:
        hp = None

    # Get meta file and process dates
    meta = GetMeta(Data)
    meta["date"] = pd.to_datetime(meta["date"])
    meta["status"] = meta["status"].astype(bool)
    dates = dates[~dates.isin(meta.loc[meta["status"] == 1, "date"])]
    print(f"📊 Found {len(dates)} dates to be fetched")

    # Process each date
    for d in tqdm(dates):
        try:
            processing_date = d.date()
            day = f"{processing_date.day:02d}"
            month = f"{processing_date.month:02d}"
            year = f"{processing_date.year}"
            file_name = f"{year}{month}{day}"
            
            nc_folder_path = f"{Data}/nc/{file_name}"
            zip_file_path = f"{Data}/zip/{file_name}.zip"
            
            if os.path.exists(nc_folder_path) and os.path.exists(zip_file_path):
                ds = zip_to_nc(zip_file_path, nc_folder_path)
            else:
                fetch_data(processing_date, processing_date, zip_file_path, area=area)
                check(nc_folder_path)
                ds = zip_to_nc(zip_file_path, nc_folder_path)
            
            if ds.isnull().to_array().sum().values > 0:
                meta = pd.concat([meta, pd.DataFrame({"date": [d], "status": [False]})], ignore_index=True)
                meta = meta.drop_duplicates(subset="date", keep="last")
                meta.to_csv(f"{Data}/meta.csv", index=False)
                print(f"⚠️  Data for {processing_date} has missing values")
            else:
                NCtoPWW(ds, f"{Data}/pww/daily/{file_name}.pww", region_name)
                meta = pd.concat([meta, pd.DataFrame({"date": [d], "status": [True]})], ignore_index=True)
                meta = meta.drop_duplicates(subset="date", keep="last")
                meta.to_csv(f"{Data}/meta.csv", index=False)
                
        except Exception as e:
            meta = pd.concat([meta, pd.DataFrame({"date": [d], "status": [False]})], ignore_index=True)
            meta = meta.drop_duplicates(subset="date", keep="last")
            meta.to_csv(f"{Data}/meta.csv", index=False)
            print(f"❌ Error processing {processing_date}: {e}")

    # Pack quarterly data
    if start_date and end_date:
        quarter_date = start_date + (end_date - start_date) / 2
    elif start_date:
        quarter_date = start_date
    else:
        quarter_date = None
        
    ds, file_name = pack_quarter(quarter_date)
    NCtoPWW(ds, f"{Data}/pww/quarter/{file_name}.pww", region_name)
    
    print(f"🎉 Pipeline completed successfully!")

# =============================================================================
# 🎯 EASY CONFIGURATION SECTION - JUST CHANGE THESE VALUES!
# =============================================================================

if __name__ == "__main__":
    # Predefined coordinate sets for easy copy/paste:
    HAWAII = ["23", "-161", "18", "-154"]
    CONUS = ["58", "-130", "24", "-60"]  # Continental US
    TEXAS = ["37", "-107", "25", "-93"]
    CALIFORNIA = ["42", "-125", "32", "-114"]
    FLORIDA = ["31", "-87", "24", "-80"]
    NORTHEAST = ["48", "-80", "40", "-66"]
    
    # 🔧 CHANGE THESE SETTINGS:
    AREA = HAWAII                           # ← Change this to your desired area
    REGION_NAME = "hawaii"                  # ← Change this to match your region
    START_DATE = datetime(2025, 8, 1)      # ← Change start date
    END_DATE = datetime(2025, 8, 31)        # ← Change end date
    
    # 🚀 RUN THE COMPLETE PIPELINE:
    main(
        start_date=START_DATE,
        end_date=END_DATE, 
        area=AREA,
        region_name=REGION_NAME
    )
    
    # Other examples:
    # main(area=CALIFORNIA, region_name="california")  # Default dates (last 2 weeks)
    # main(start_date=datetime(2025, 9, 1), area=TEXAS, region_name="texas")  # Single date
    # main(area=["30", "-100", "25", "-95"], region_name="custom")  # Custom coordinates