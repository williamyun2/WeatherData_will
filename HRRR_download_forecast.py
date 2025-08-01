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


def main():
    """Main data fetching and processing routine."""
    ensure_directories()

    # Get today's 12Z
    now = datetime.now()
    target_date = now.replace(hour=12, minute=0, second=0, microsecond=0)
    
    # If it's before 12Z today, use yesterday's 12Z
    if now.hour < 12:
        target_date -= timedelta(days=1)
    
    date_iso = target_date.strftime("%Y-%m-%dT12:00:00")
    pww_date = target_date.strftime("%Y-%m-%dT12Z")

    logger.info(f"Preparing to download {PRODUCT} data for {date_iso}")

    # Check if already processed
    meta_file = os.path.join(DATA_DIR, "meta.csv")
    if os.path.exists(meta_file):
        meta = pd.read_csv(meta_file)
        meta["date"] = pd.to_datetime(meta["date"])
        if target_date in meta[meta["status"] == True]["date"].values:
            logger.info(f"Data for {date_iso} already processed. Exiting.")
            return

    gauth = GoogleAuth(settings_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.yaml"))
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)
    hp = helper(logger)

    # Process single date
    try:
        # Download gribs data
        H = FastHerbie(
            [date_iso],
            model="hrrr",
            product=PRODUCT,
            fxx=range(1, 49),
            save_dir=GRIB_FOLDER,
        )
        H.download(REGEX)
        
        # Process and save
        ds = get_multiple_HRRR([date_iso], list(range(1, 49)), PRODUCT, REGEX, GRIB_FOLDER)
        ds = hrrr_process(ds)

        file_name = f"{pww_date}_{PRODUCT}_48_{STATE}.pww"
        NC2PWW(ds, os.path.join(PWW_DAILY_FOLDER, file_name))
        
        zip_file = os.path.join(ZIP_FOLDER, f"{pww_date}_{PRODUCT}_48_{STATE}.zip")
        hp.zip_file(os.path.join(PWW_DAILY_FOLDER, file_name), zip_file, remove=False)

        logger.info(f"Processed {file_name}")
        
        # Upload to Google Drive
        logger.info(f"Uploading {zip_file} to Google Drive...")
        hp.upload_to_drive(drive, "1M6m4r7cfH6Vbg1yBP7P-b7IicUrfC6S_", zip_file)
        
        status = True
    except Exception as e:
        logger.error(f"Failed to process {date_iso}: {e}")
        status = False

    # Update meta
    meta = pd.DataFrame({"date": [target_date], "status": [status]})
    if os.path.exists(meta_file):
        existing_meta = pd.read_csv(meta_file)
        meta = pd.concat([existing_meta, meta], ignore_index=True)
        meta = meta.drop_duplicates(subset="date", keep="last")
    meta.to_csv(meta_file, index=False)



if __name__ == "__main__":
    main()
