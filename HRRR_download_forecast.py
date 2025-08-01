# author: Thomas Chen 
# date: 2025-05-01
# description: This script automates the download and processing of HRRR weather forecast data.
# It fetches data for the last 24 hours, processes it into a specific format, and uploads it to Google Drive.
# version: 1.2
# notes: with the help of AI, this script has been improved for better readability and efficiency.
# =========================

import os
os.environ['PYTHONIOENCODING'] = 'utf-8'
import sys
import time
import logging
from datetime import datetime, timedelta
import regionmask

import numpy as np
import pandas as pd
from tqdm import tqdm

from herbie import Herbie, FastHerbie
from HRRR_auto import (
    HiddenPrints,
    get_multiple_HRRR,
    get_single_HRRR,
    hrrr_process,
    NC2PWW,
)
from helper import helper

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

from datetime import datetime, timedelta
import pandas as pd
import os
# =========================
# Configuration and Globals
# =========================

# Set timezone to America/Chicago
if os.name != "nt":
    os.environ["TZ"] = "America/Chicago"
    time.tzset()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Herbie/HRRR configuration
PRODUCT = "sfc"
STATE = "TX"  # US for CONUS
REGEX = r":(?:TMP|DPT|UGRD|VGRD|TCDC|DSWRF|COLMD|GUST|CPOFP|PRATE):((2|8|10|80) m above|entire atmosphere|surface|entire atmosphere single layer)"
# REGEX = r":(?:TMP|DPT|UGRD|VGRD|TCDC|DSWRF|COLMD|GUST|CPOFP|PRATE):(?:(?:2|8|10|80) m above ground|entire atmosphere|surface|entire atmosphere \(considered as a single layer\))"

GRIB_FOLDER = os.path.join(DATA_DIR, "grib")
NC_FOLDER = os.path.join(DATA_DIR, "nc")
PWW_DAILY_FOLDER = os.path.join(DATA_DIR, "pww", "daily")
ZIP_FOLDER = os.path.join(DATA_DIR, "zip")

# =========================
# Logging Setup
# =========================


LOG_FILE = os.path.join(BASE_DIR, "process.log")

# Create handlers with explicit UTF-8 encoding
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
console_handler = logging.StreamHandler(sys.stdout)

# Configure console handler for UTF-8
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    if hasattr(console_handler.stream, 'reconfigure'):
        console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    pass  # Fallback if reconfigure not available

# Set up logging with custom handlers
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, console_handler],
    force=True  # Override any existing configuration
)
logger = logging.getLogger(__name__)




# =========================
# Utility Functions
# =========================

def ensure_directories():
    """Ensure necessary directories exist."""
    for path in [NC_FOLDER, PWW_DAILY_FOLDER, ZIP_FOLDER, GRIB_FOLDER]:
        os.makedirs(path, exist_ok=True)


def get_target_dates(meta_file, lag_days):
    """
    Get list of target dates (6-hourly) to fetch (not already processed).
    Only matches on year, month, day, and hour (ignores minutes/seconds).
    """
    # Read meta file or create empty DataFrame
    if os.path.exists(meta_file):
        meta = pd.read_csv(meta_file)
    else:
        meta = pd.DataFrame(columns=["date", "status"])
    meta["date"] = pd.to_datetime(meta["date"], errors='coerce')
    meta["status"] = meta["status"].astype(bool)
    meta["date_hour"] = meta["date"].dt.floor('h')
    # Find the most recent 6-hour boundary before now
    now = datetime.now()
    last_6h = now.replace(hour=(now.hour // 6) * 6, minute=0, second=0, microsecond=0)
    all_dates = pd.date_range(last_6h - timedelta(days=lag_days), last_6h, freq="6h")
    # Only keep dates not already processed (by hour)
    processed_hours = set(meta.loc[meta["status"], "date_hour"])
    pending_dates = [d for d in all_dates if d not in processed_hours]

    return pending_dates, meta

# =========================
# Main Processing Function
# =========================


def subset_to_texas(ds):
    """
    Subset xarray dataset to Texas boundaries using state shapefile.
    """
    import regionmask
    
    # Debug: print coordinate info
    print(f"Dataset coordinates: {list(ds.coords)}")
    print(f"Dataset dims: {list(ds.dims)}")
    print(f"Longitude range: {ds.longitude.min().values} to {ds.longitude.max().values}")
    print(f"Latitude range: {ds.latitude.min().values} to {ds.latitude.max().values}")
    
    # Convert longitude to -180 to 180 if needed (HRRR uses 0-360)
    lon = ds.longitude
    if lon.max() > 180:
        lon = lon.where(lon <= 180, lon - 360)
    
    # Get US states from regionmask (uses Natural Earth data)
    us_states = regionmask.defined_regions.natural_earth_v5_0_0.us_states_50
    
    # Create mask for Texas using corrected coordinates
    try:
        texas_mask = us_states.mask(lon, ds.latitude) == us_states.map_keys('TX')
        print("Texas mask created successfully")
    except Exception as e:
        print(f"Error creating mask: {e}")
        # Fallback: use bounding box method
        print("Falling back to bounding box method...")
        texas_bounds = {
            'lat_min': 25.8, 'lat_max': 36.5,
            'lon_min': -106.6, 'lon_max': -93.5
        }
        texas_mask = (
            (ds.latitude >= texas_bounds['lat_min']) & 
            (ds.latitude <= texas_bounds['lat_max']) &
            (lon >= texas_bounds['lon_min']) & 
            (lon <= texas_bounds['lon_max'])
        )
    
    # Apply mask and crop to reduce file size
    ds_texas = ds.where(texas_mask, drop=True)
    
    print(f"Original dataset shape: {ds.dims}")
    print(f"Texas dataset shape: {ds_texas.dims}")
    
    return ds_texas


def main():
    """Main data fetching and processing routine - processes both TX and US."""
    ensure_directories()

    # Get today's 12Z
    now = datetime.now()
    target_date = now.replace(hour=12, minute=0, second=0, microsecond=0)
    
    # If it's before 12Z today, use yesterday's 12Z
    if now.hour < 12:
        target_date -= timedelta(days=1)
    
    date_iso = target_date.strftime("%Y-%m-%dT12:00:00")
    pww_date = target_date.strftime("%Y-%m-%dT12Z")

    logger.info(f"Preparing to download sfc data for {date_iso} - Processing both TX and US")

    # Check if already processed - SIMPLE VERSION
    meta_file = os.path.join(DATA_DIR, "meta.csv")
    if os.path.exists(meta_file):
        meta = pd.read_csv(meta_file)
        meta["date"] = pd.to_datetime(meta["date"])
        
        # Check if both TX and US are already processed for this date
        tx_done = len(meta[(meta["date"] == target_date) & 
                          (meta["region"] == "TX") & 
                          (meta["status"] == True)]) > 0
        us_done = len(meta[(meta["date"] == target_date) & 
                          (meta["region"] == "US") & 
                          (meta["status"] == True)]) > 0
        
        if tx_done and us_done:
            logger.info(f"Data for {date_iso} already processed for both TX and US regions. Exiting.")
            return

    gauth = GoogleAuth(settings_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.yaml"))
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)
    hp = helper(logger)

    # Download data once
    try:
        logger.info("Downloading GRIB data...")
        H = FastHerbie(
            [date_iso],
            model="hrrr",
            product=PRODUCT,
            fxx=range(1, 49),
            save_dir=GRIB_FOLDER,
        )
        H.download(REGEX)
        
        # Get the raw dataset (full CONUS)
        logger.info("Loading raw dataset...")
        ds = get_multiple_HRRR([date_iso], list(range(1, 49)), PRODUCT, REGEX, GRIB_FOLDER)
        
        # Define regions to process
        regions = [
            {
                'name': 'TX',
                'folder_id': '1Spsi9cgmqzX8WVS4uZ68Apq2r1x_Hg7g',  # Texas folder
                'process_func': subset_to_texas  # Apply Texas subsetting
            },
            {
                'name': 'US', 
                'folder_id': '1M6m4r7cfH6Vbg1yBP7P-b7IicUrfC6S_',  # US folder
                'process_func': lambda x: x      # No subsetting (full CONUS)
            }
        ]
        
        # Process each region
        all_success = True
        meta_entries = []
        
        for region in regions:
            region_name = region['name']
            try:
                logger.info(f"Processing {region_name} data...")
                
                # Apply region-specific processing (subset or not)
                ds_region = region['process_func'](ds.copy())
                
                # Apply HRRR processing (unit conversions, etc.)
                ds_processed = hrrr_process(ds_region)

                # Create PWW file
                file_name = f"{pww_date}_{PRODUCT}_48_{region_name}.pww"
                pww_path = os.path.join(PWW_DAILY_FOLDER, file_name)
                NC2PWW(ds_processed, pww_path)
                
                # Create zip file
                zip_file = os.path.join(ZIP_FOLDER, f"{pww_date}_{PRODUCT}_48_{region_name}.zip")
                hp.zip_file(pww_path, zip_file, remove=False)

                logger.info(f"Processed {file_name}")
                
                # Upload to Google Drive
                logger.info(f"Uploading {region_name} data to Google Drive...")
                hp.upload_to_drive(drive, region['folder_id'], zip_file)
                
                logger.info(f"✅ Successfully completed {region_name} processing")
                meta_entries.append({"date": target_date, "status": True, "region": region_name})
                
            except Exception as e:
                logger.error(f"❌ Failed to process {region_name}: {e}")
                meta_entries.append({"date": target_date, "status": False, "region": region_name})
                all_success = False

        logger.info(f"Overall status: {'✅ Success' if all_success else '⚠️ Partial success'}")
        
    except Exception as e:
        logger.error(f"Failed to download/setup data for {date_iso}: {e}")
        # Record failure for both regions
        meta_entries = [
            {"date": target_date, "status": False, "region": "TX"},
            {"date": target_date, "status": False, "region": "US"}
        ]

    # Update meta file
    if meta_entries:
        new_meta = pd.DataFrame(meta_entries)
        if os.path.exists(meta_file):
            existing_meta = pd.read_csv(meta_file)
            meta = pd.concat([existing_meta, new_meta], ignore_index=True)
            meta = meta.drop_duplicates(subset=["date", "region"], keep="last")
        else:
            meta = new_meta
        meta.to_csv(meta_file, index=False)
        logger.info("Updated meta.csv with processing results")



        
if __name__ == "__main__":
    main()
