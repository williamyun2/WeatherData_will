import xarray as xr
from herbie import Herbie, FastHerbie
import os
import sys, warnings
import numpy as np
from hrrr_auto import HiddenPrints, get_multiple_HRRR, hrrr_process, NC2PWW
from helper import helper
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
import numpy as np
from typing import Callable
import pickle
import struct
import multiprocessing as mp
from glob import glob
import re
import logging
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import shutil

warnings.filterwarnings("ignore", message="This pattern is interpreted as a regular expression, and has match groups.")

# =========================
# Configuration and Globals
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
HISTORICAL_PWW_FOLDER = os.path.join(DATA_DIR, "historical_pww")
HISTORICAL_ZIP_FOLDER = os.path.join(DATA_DIR, "historical_zip")
GRIB_FOLDER = os.path.join(DATA_DIR, "grib")

# Google Drive folder ID for historical data
# daily daily 
# HISTORICAL_DRIVE_FOLDER_ID = "1Uc-tuSPEnh7rJzC3nFvxndFvULrsNe-U"

# monthly monthly monthly monthly 
HISTORICAL_DRIVE_FOLDER_ID = "1_govjuY2WV0TqHp_7PwVVtrGPCDU-I9v"

# =========================
# Logging Setup
# =========================

LOG_FILE = os.path.join(BASE_DIR, "historical_process.log")

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
    for path in [HISTORICAL_PWW_FOLDER, HISTORICAL_ZIP_FOLDER, GRIB_FOLDER, 
                 os.path.join(BASE_DIR, "state_station")]:
        os.makedirs(path, exist_ok=True)

def setup_google_drive():
    """Setup Google Drive authentication and return drive object."""
    try:
        gauth = GoogleAuth(settings_file=os.path.join(BASE_DIR, "settings.yaml"))
        gauth.ServiceAuth()
        drive = GoogleDrive(gauth)
        return drive
    except Exception as e:
        logger.error(f"Failed to setup Google Drive: {e}")
        return None

def download_HRRR_fast(date_, fxx_):
    """Download HRRR data using FastHerbie."""
    try:
        H = FastHerbie(
            date_,
            model="hrrr",
            product="sfc",
            fxx=fxx_,
            save_dir=GRIB_FOLDER,
        )
        regex = r":(?:TMP|DPT|UGRD|VGRD|TCDC|DSWRF|COLMD|GUST|CPOFP|PRATE):((2|8|10|80) m above|entire atmosphere|surface|entire atmosphere single layer)"
        H.download(regex)
        del H
    except Exception as e:
        logger.error(f"Error in fetching data for {date_}: {e}")

def process_and_upload(target_date, fxx, product, regex, state, drive, hp, mode="day"):
    """
    Process HRRR data and upload to Google Drive.
    
    Args:
        target_date: Target date/month to process
        fxx: Forecast hour
        product: HRRR product type
        regex: Regex pattern for variables
        state: State/region to process
        drive: Google Drive object
        hp: Helper object
        mode: "day" or "month" processing mode
    """
    if isinstance(target_date, str):
        target_date = pd.to_datetime(target_date)
    
    if mode == "day":
        # Create 24 hours for the target day
        dates = pd.date_range(start=target_date, freq="h", periods=24)
        file_date = target_date.strftime("%Y_%m_%d")
        file_name = f"{state}_{file_date}.pww"
        description = f"1 day ({len(dates)} hours): {target_date.strftime('%Y-%m-%d')}"
    elif mode == "month":
        # Create all hours for the entire month
        dates = pd.date_range(
            start=target_date, 
            end=target_date + pd.DateOffset(months=1), 
            freq="h", 
            inclusive="both"
        ) - pd.Timedelta(hours=1)
        file_date = target_date.strftime("%Y_%m")
        file_name = f"{state}{file_date}.pww"  # Same format as original
        description = f"1 month ({len(dates)} hours): {target_date.strftime('%Y-%m')}"
    else:
        raise ValueError("Mode must be 'day' or 'month'")
    
    pww_path = os.path.join(HISTORICAL_PWW_FOLDER, file_name)
    zip_name = file_name.replace('.pww', '.zip')
    zip_path = os.path.join(HISTORICAL_ZIP_FOLDER, zip_name)
    
    logger.info(f"Processing {description}")
    
    try:
        # Check if ZIP already exists locally first
        if os.path.exists(zip_path):
            logger.info(f"{zip_name} already exists locally.")
            
            # Still check if needs upload to Google Drive
            if drive:
                cloud_files = drive.ListFile({"q": f"'{HISTORICAL_DRIVE_FOLDER_ID}' in parents and trashed=false"}).GetList()
                cloud_files_dict = {file["title"]: file for file in cloud_files}
                
                if zip_name not in cloud_files_dict:
                    logger.info(f"Uploading existing {zip_name} to Google Drive...")
                    hp.upload_to_drive(drive, HISTORICAL_DRIVE_FOLDER_ID, zip_path)
                    logger.info(f"✅ Successfully uploaded existing {zip_name}")
                else:
                    logger.info(f"{zip_name} already exists on Google Drive too. Skipping completely.")
            
            return True
        
        # Check if already exists on Google Drive (but not locally)
        if drive:
            cloud_files = drive.ListFile({"q": f"'{HISTORICAL_DRIVE_FOLDER_ID}' in parents and trashed=false"}).GetList()
            cloud_files_dict = {file["title"]: file for file in cloud_files}
            
            if zip_name in cloud_files_dict:
                logger.info(f"{zip_name} already exists in Google Drive. Skipping processing.")
                return True
        
        # Download GRIB data
        logger.info(f"Downloading GRIB data for {description}...")
        download_HRRR_fast(dates, fxx_=[fxx])
        
        # Process data
        logger.info(f"Processing weather data...")
        ds = get_multiple_HRRR(dates, fxx, product, regex, GRIB_FOLDER)
        
        if ds is not None:
            # Apply HRRR processing
            processed_ds = hrrr_process(ds)
            
            # Create PWW file
            logger.info(f"Creating PWW file: {file_name}")
            NC2PWW(processed_ds, pww_path, state)
            
            # Create zip file
            logger.info(f"Compressing to: {zip_name}")
            hp.zip_file(pww_path, zip_path, remove=True)  # Remove PWW after zipping
            
            # Upload to Google Drive
            if drive:
                logger.info(f"Uploading {zip_name} to Google Drive...")
                hp.upload_to_drive(drive, HISTORICAL_DRIVE_FOLDER_ID, zip_path)
                logger.info(f"✅ Successfully uploaded {zip_name}")
                
                # Optionally remove local zip file after upload
                # os.remove(zip_path)
            else:
                logger.warning("Google Drive not available, skipping upload")
            
            logger.info(f"✅ Successfully processed: {file_name}")
            return True
        else:
            logger.error(f"❌ No data retrieved for {target_date}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error processing {target_date}: {e}")
        return False

def process_one_day(target_date, fxx, product, regex, state, drive=None, hp=None):
    """Process exactly one day (24 hours)"""
    return process_and_upload(target_date, fxx, product, regex, state, drive, hp, mode="day")

def process_one_month(target_month, fxx, product, regex, state, drive=None, hp=None):
    """Process exactly one month (all hours in that month)"""
    return process_and_upload(target_month, fxx, product, regex, state, drive, hp, mode="month")

def cleanup_grib():
    """Clean up GRIB files after processing."""
    try:
        if os.path.exists(GRIB_FOLDER):
            shutil.rmtree(GRIB_FOLDER)
            os.makedirs(GRIB_FOLDER, exist_ok=True)
            logger.info("🧹 GRIB files cleaned up.")
    except Exception as e:
        logger.warning(f"Failed to clean up GRIB files: {e}")

def process_date_range_with_cleanup(start_date, end_date, fxx, product, regex, state, drive=None, hp=None, mode="day"):
    """
    Process a range of dates with GRIB cleanup after each processing unit.
    """
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    
    if mode == "day":
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        process_func = process_one_day
    elif mode == "month":
        date_range = pd.date_range(start=start_date.replace(day=1), 
                                  end=end_date.replace(day=1), 
                                  freq="MS")
        process_func = process_one_month
    else:
        raise ValueError("Mode must be 'day' or 'month'")
    
    successful = 0
    failed = 0
    
    logger.info(f"Processing {len(date_range)} {mode}(s) from {start_date.date()} to {end_date.date()}")
    
    for i, date in enumerate(date_range, 1):
        logger.info(f"📅 Processing {mode} {i}/{len(date_range)}: {date.strftime('%Y-%m-%d')}")
        
        success = process_func(date, fxx, product, regex, state, drive, hp)
        if success:
            successful += 1
        else:
            failed += 1
            
        # Clean up GRIB files after each processing unit
        cleanup_grib()
    
    logger.info(f"Summary: {successful} successful, {failed} failed")
    return successful, failed

def manual_processing():
    """Manual processing function for specific dates/ranges."""
    # Ensure directories exist
    ensure_directories()
    
    # Setup Google Drive and helper - DISABLED FOR DOWNLOAD ONLY
    drive = None  # Set to None to skip all Google Drive operations
    hp = helper(logger)
    
    # Configuration
    product = "sfc"
    fxx = 1
    state = "CONUS"
    regex = r":(?:TMP|DPT|UGRD|VGRD|TCDC|DSWRF|COLMD|GUST|CPOFP|PRATE):((2|8|10|80) m above|entire atmosphere|surface|entire atmosphere single layer)"

    # =================================================================
    # MANUAL PROCESSING OPTIONS - UNCOMMENT WHAT YOU NEED
    # =================================================================
    
    # OPTION 1: Process exactly 1 day (24 hours)
    # process_one_day("2025-08-13", fxx, product, regex, state, drive, hp)
    
    # OPTION 2: Process exactly 1 month (all hours in month)
    # process_one_month("2025-09-01", fxx, product, regex, state, drive, hp)
    
    # OPTION 3: Process a range of days with cleanup
    # process_date_range_with_cleanup("2025-07-01", "2025-08-12", fxx, product, regex, state, drive, hp, mode="day")
    
    # OPTION 4: Process a range of months with cleanup after each month
    process_date_range_with_cleanup("2025-01-01", "2025-06-01", fxx, product, regex, state, drive, hp, mode="month")
    
    logger.info("Manual processing complete! Files saved locally only.")
    logger.info(f"ZIP files location: {HISTORICAL_ZIP_FOLDER}")


def main():
    """Main function to process yesterday's data automatically."""
    # Ensure directories exist
    ensure_directories()
    
    # Setup Google Drive and helper
    drive = setup_google_drive()
    hp = helper(logger)
    
    # Configuration
    product = "sfc"
    fxx = 1
    state = "CONUS"
    regex = r":(?:TMP|DPT|UGRD|VGRD|TCDC|DSWRF|COLMD|GUST|CPOFP|PRATE):((2|8|10|80) m above|entire atmosphere|surface|entire atmosphere single layer)"

    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    
    logger.info(f"Starting historical processing for yesterday: {yesterday_str}")
    
    # Process yesterday's data
    success = process_one_day(yesterday_str, fxx, product, regex, state, drive, hp)
    
    if success:
        logger.info(f"✅ Successfully processed historical data for {yesterday_str}")
    else:
        logger.error(f"❌ Failed to process historical data for {yesterday_str}")
    
    logger.info("Historical processing complete!")
    return success

if __name__ == "__main__":
    import sys
    
    # Check if manual mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--manual":
        manual_processing()
    else:
        # Default: process yesterday's data (for crontab)
        main()



# https://claude.ai/chat/8e53c1cf-391f-4728-b449-12c56982b409
