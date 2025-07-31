# author: Thomas Chen 
# date: 2025-05-01
# description: This script automates the download and processing of HRRR weather forecast data.
# It fetches historical data from Jan 1, 2025 to present, processes it into PWW format, and uploads to Google Drive.
# version: 1.3
# notes: with the help of AI, this script has been improved for better readability and efficiency.
# =========================

import os
import sys
import time
import logging
from datetime import datetime, timedelta

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

# =========================
# Configuration and Globals
# =========================

# Set encoding and timezone
os.environ['PYTHONIOENCODING'] = 'utf-8'

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
MODEL_HOUR = "12"  # Model run hour (12Z)
REGEX = r":(?:TMP|DPT|UGRD|VGRD|TCDC|DSWRF|COLMD|GUST|CPOFP|PRATE):((2|8|10|80) m above|entire atmosphere|surface|entire atmosphere single layer)"

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

def get_historical_dates(start_date, end_date, meta_file):
    """Get all dates from start_date to end_date that need processing."""
    # Read existing meta
    if os.path.exists(meta_file):
        meta = pd.read_csv(meta_file)
        meta["date"] = pd.to_datetime(meta["date"], errors='coerce')
        meta["status"] = meta["status"].astype(bool)
        processed_dates = set(meta.loc[meta["status"], "date"].dt.floor('D'))
    else:
        meta = pd.DataFrame(columns=["date", "status"])
        processed_dates = set()
    
    # Generate all dates from start to end (daily at specified hour only)
    all_dates = pd.date_range(
        start=start_date, 
        end=end_date, 
        freq="D"  # Daily instead of 6-hourly
    )
    
    # Add specified hour to each date
    all_dates = [d.replace(hour=int(MODEL_HOUR)) for d in all_dates]
    
    # Filter out already processed
    pending_dates = [d for d in all_dates if d.floor('D') not in processed_dates]
    
    return pending_dates, meta

# =========================
# Main Processing Function
# =========================

def main():
    ensure_directories()
    
    # Historical backfill configuration
    START_DATE = datetime(2025, 1, 1)  # Jan 1, 2025
    END_DATE = datetime.now() - timedelta(days=2)  # Up to 2 days ago
    
    # Setup auth
    gauth = GoogleAuth(settings_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.yaml"))
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)
    hp = helper(logger)
    
    # Get historical dates to process
    meta_file = os.path.join(DATA_DIR, "meta.csv")
    dates, meta = get_historical_dates(START_DATE, END_DATE, meta_file)
    
    logger.info(f"Found {len(dates)} historical dates to process")
    
    # Process in batches (don't overwhelm NOAA servers)
    BATCH_SIZE = 5  # Process 5 days at a time
    
    for i in range(0, len(dates), BATCH_SIZE):
        batch = dates[i:i+BATCH_SIZE]
        logger.info(f"Processing batch {i//BATCH_SIZE + 1}: {len(batch)} dates")
        
        for target_date in tqdm(batch, desc=f"Batch {i//BATCH_SIZE + 1}"):
            # Create date strings using MODEL_HOUR variable
            date_iso = target_date.strftime("%Y-%m-%d") + f"T{MODEL_HOUR}:00:00"
            pww_date = target_date.strftime(f"%Y-%m-%dT{MODEL_HOUR}Z")
            
            try:
                # Download and process
                H = FastHerbie([date_iso], model="hrrr", product=PRODUCT, fxx=range(1, 49), save_dir=GRIB_FOLDER)
                H.download(REGEX)
                ds = get_multiple_HRRR([date_iso], list(range(1, 49)), PRODUCT, REGEX, GRIB_FOLDER)
                ds = hrrr_process(ds)
                
                file_name = f"{pww_date}_{PRODUCT}_48_{STATE}.pww"
                NC2PWW(ds, os.path.join(PWW_DAILY_FOLDER, file_name))
                
                zip_file = os.path.join(ZIP_FOLDER, f"{pww_date}_{PRODUCT}_48_{STATE}.zip")
                hp.zip_file(os.path.join(PWW_DAILY_FOLDER, file_name), zip_file, remove=False)
                
                status = True
                logger.info(f"Processed historical {file_name}")
                
            except Exception as e:
                logger.error(f"Failed to process {date_iso}: {e}")
                status = False
            
            # Update meta
            meta = pd.concat([meta, pd.DataFrame({"date": [target_date], "status": [status]})], ignore_index=True)
            meta = meta.drop_duplicates(subset="date", keep="last")
            meta.to_csv(meta_file, index=False)
            
            # Rate limiting - be nice to NOAA servers
            time.sleep(2)
        
        # Upload batch to drive
        hp.upload_to_drive(drive, "1zl-xxQHVB0lqvum_eVZnIpizDvAaq1N7", os.path.join(ZIP_FOLDER, "*.zip"))
        
        # Optional: Clean up local files after upload to save space
        # shutil.rmtree(GRIB_FOLDER)
        # os.makedirs(GRIB_FOLDER, exist_ok=True)

if __name__ == "__main__":
    main()
