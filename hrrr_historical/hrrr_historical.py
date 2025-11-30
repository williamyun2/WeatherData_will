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
warnings.filterwarnings("ignore", category=FutureWarning, message="In a future version of xarray the default value for compat will change")

# =========================
# Configuration and Globals
# =========================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
HISTORICAL_PWW_FOLDER = os.path.join(DATA_DIR, "historical_pww")
HISTORICAL_ZIP_FOLDER = os.path.join(DATA_DIR, "historical_zip")
GRIB_FOLDER = os.path.join(DATA_DIR, "grib")

# PRODUCTION PRODUCTION PRODUCTION PRODUCTION PRODUCTION PRODUCTION PRODUCTION 
DAILY_DRIVE_FOLDER_ID = "1Uc-tuSPEnh7rJzC3nFvxndFvULrsNe-U"
MONTHLY_DRIVE_FOLDER_ID = "1_govjuY2WV0TqHp_7PwVVtrGPCDU-I9v"
ARCHIVE_DRIVE_FOLDER_ID = "1yH-PC52yq2GFsymW5mdENTcsMvu27U0E"

# PERSONAL TEST FOLDERS
# DAILY_DRIVE_FOLDER_ID = "1tiKQf168JP36Mfjh0tHmRhDmQde03MJg"      
# MONTHLY_DRIVE_FOLDER_ID = "1Ob8z9gX9Btvs5K178LxrIYXciMDIzB9s"    
# ARCHIVE_DRIVE_FOLDER_ID = "1f-8gcb0T5TRfdRJVh_PPkYhtY2DLk15b"   

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

def get_drive_folder_id(mode):
    """Get the appropriate Google Drive folder ID based on processing mode."""
    if mode == "day":
        return DAILY_DRIVE_FOLDER_ID
    elif mode == "month":
        return MONTHLY_DRIVE_FOLDER_ID
    elif mode == "archive":
        return ARCHIVE_DRIVE_FOLDER_ID
    else:
        raise ValueError("Mode must be 'day', 'month', or 'archive'")

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
    Automatically routes to correct folder based on mode.
    
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
    
    # Get appropriate folder ID based on mode
    folder_id = get_drive_folder_id(mode)
    
    if mode == "day":
        # Create 24 hours for the target day
        dates = pd.date_range(start=target_date, freq="h", periods=24)
        file_date = target_date.strftime("%Y_%m_%d")
        file_name = f"{state}_{file_date}.pww"
        description = f"1 day ({len(dates)} hours): {target_date.strftime('%Y-%m-%d')}"
        folder_name = "daily"
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
        folder_name = "monthly"
    else:
        raise ValueError("Mode must be 'day' or 'month'")
    
    pww_path = os.path.join(HISTORICAL_PWW_FOLDER, file_name)
    zip_name = file_name.replace('.pww', '.zip')
    zip_path = os.path.join(HISTORICAL_ZIP_FOLDER, zip_name)
    
    logger.info(f"Processing {description}")
    logger.info(f"Will upload to {folder_name} folder on Google Drive")
    
    try:
        # Check if ZIP already exists locally first
        if os.path.exists(zip_path):
            logger.info(f"{zip_name} already exists locally.")
            
            # Still check if needs upload to Google Drive
            if drive:
                cloud_files = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()
                cloud_files_dict = {file["title"]: file for file in cloud_files}
                
                if zip_name not in cloud_files_dict:
                    logger.info(f"Uploading existing {zip_name} to Google Drive {folder_name} folder...")
                    hp.upload_to_drive(drive, folder_id, zip_path)
                    logger.info(f"Successfully uploaded existing {zip_name}")
                else:
                    logger.info(f"{zip_name} already exists on Google Drive too. Skipping completely.")
            
            return True
        
        # Check if already exists on Google Drive (but not locally)
        if drive:
            cloud_files = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()
            cloud_files_dict = {file["title"]: file for file in cloud_files}
            
            if zip_name in cloud_files_dict:
                logger.info(f"{zip_name} already exists in Google Drive {folder_name} folder. Skipping processing.")
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
                logger.info(f"Uploading {zip_name} to Google Drive {folder_name} folder...")
                hp.upload_to_drive(drive, folder_id, zip_path)
                logger.info(f"Successfully uploaded {zip_name}")
                
                # Optionally remove local zip file after upload
                # os.remove(zip_path)
            else:
                logger.warning("Google Drive not available, skipping upload")
            
            logger.info(f"Successfully processed: {file_name}")
            return True
        else:
            logger.error(f"No data retrieved for {target_date}")
            return False
            
    except Exception as e:
        logger.error(f"Error processing {target_date}: {e}")
        return False

def process_one_day(target_date, fxx, product, regex, state, drive=None, hp=None):
    """Process exactly one day (24 hours) - uploads to daily folder"""
    return process_and_upload(target_date, fxx, product, regex, state, drive, hp, mode="day")

def process_one_month(target_month, fxx, product, regex, state, drive=None, hp=None):
    """Process exactly one month (all hours in that month) - uploads to monthly folder"""
    return process_and_upload(target_month, fxx, product, regex, state, drive, hp, mode="month")

def cleanup_grib():
    """Clean up GRIB files after processing."""
    try:
        if os.path.exists(GRIB_FOLDER):
            shutil.rmtree(GRIB_FOLDER)
            os.makedirs(GRIB_FOLDER, exist_ok=True)
            logger.info("GRIB files cleaned up.")
    except Exception as e:
        logger.warning(f"Failed to clean up GRIB files: {e}")

def process_date_range_with_cleanup(start_date, end_date, fxx, product, regex, state, drive=None, hp=None, mode="day"):
    """
    Process a range of dates with GRIB cleanup after each processing unit.
    Automatically routes to correct Google Drive folder based on mode.
    """
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)
    
    if mode == "day":
        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        process_func = process_one_day
        folder_name = "daily"
    elif mode == "month":
        date_range = pd.date_range(start=start_date.replace(day=1), 
                                  end=end_date.replace(day=1), 
                                  freq="MS")
        process_func = process_one_month
        folder_name = "monthly"
    else:
        raise ValueError("Mode must be 'day' or 'month'")
    
    successful = 0
    failed = 0
    
    logger.info(f"Processing {len(date_range)} {mode}(s) from {start_date.date()} to {end_date.date()}")
    logger.info(f"Files will be uploaded to Google Drive {folder_name} folder")
    
    for i, date in enumerate(date_range, 1):
        logger.info(f"Processing {mode} {i}/{len(date_range)}: {date.strftime('%Y-%m-%d')}")
        
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
    
    # Setup Google Drive and helper - ENABLED FOR AUTOMATIC UPLOAD
    drive = setup_google_drive()  # Enable Google Drive
    hp = helper(logger)
    
    # Configuration
    product = "sfc"
    fxx = 1
    state = "CONUS"
    regex = r":(?:TMP|DPT|UGRD|VGRD|TCDC|DSWRF|COLMD|GUST|CPOFP|PRATE):((2|8|10|80) m above|entire atmosphere|surface|entire atmosphere single layer)"

    # =================================================================
    # MANUAL PROCESSING OPTIONS - UNCOMMENT WHAT YOU NEED
    # =================================================================
    
    # OPTION 1: Process exactly 1 day (24 hours) - UPLOADS TO DAILY FOLDER AUTOMATICALLY
    # process_one_day("2025-08-13", fxx, product, regex, state, drive, hp)
    
    # OPTION 2: Process exactly 1 month (all hours in month) - UPLOADS TO MONTHLY FOLDER AUTOMATICALLY
    process_one_month("2025-10-01", fxx, product, regex, state, drive, hp)
    
    # OPTION 3: Process a range of days - UPLOADS TO DAILY FOLDER AUTOMATICALLY
    # process_date_range_with_cleanup("2025-07-01", "2025-08-12", fxx, product, regex, state, drive, hp, mode="day")
    
    # OPTION 4: Process a range of months - UPLOADS TO MONTHLY FOLDER AUTOMATICALLY
    # process_date_range_with_cleanup("2025-01-01", "2025-06-01", fxx, product, regex, state, drive, hp, mode="month")
    
    logger.info("Manual processing complete!")
    logger.info(f"ZIP files location: {HISTORICAL_ZIP_FOLDER}")


def main():
    """Main function to process data automatically based on the date."""
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

    # Get current date
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    
    # FIRST: Always check and archive old daily files (using helper function)
    logger.info("Checking for old daily files to archive...")
    if drive:
        try:
            limit_timedelta = timedelta(days=30)
            date_pattern = re.compile(r"CONUS_(\d{4}_\d{2}_\d{2})")
            date_format = "%Y_%m_%d"
            
            hp.archive_folder(
                drive=drive,
                folder_id=DAILY_DRIVE_FOLDER_ID,
                archive_folder_id=ARCHIVE_DRIVE_FOLDER_ID,
                limit=limit_timedelta,
                date_pattern=date_pattern,
                date_format=date_format
            )
        except Exception as e:
            logger.error(f"Error during archive cleanup: {e}")
    else:
        logger.warning("Google Drive not available, skipping archive cleanup")
    
    # SECOND: Check for missing daily files in the past 30 days
    logger.info("Checking for missing daily files in the past 30 days...")
    missing_daily_dates = []
    
    if drive:
        try:
            # Get all files currently in the daily folder
            cloud_files = drive.ListFile({"q": f"'{DAILY_DRIVE_FOLDER_ID}' in parents and trashed=false"}).GetList()
            cloud_file_names = {file["title"] for file in cloud_files}
            
            logger.info(f"Found {len(cloud_file_names)} files in daily folder")
            
            # Check each day in the past 30 days
            for days_ago in range(1, 31):  # 1 to 30 days ago
                check_date = today - timedelta(days=days_ago)
                expected_filename = f"CONUS_{check_date.strftime('%Y_%m_%d')}.zip"
                
                if expected_filename not in cloud_file_names:
                    missing_daily_dates.append(check_date)
                    logger.info(f"Missing daily file: {expected_filename}")
            
            if missing_daily_dates:
                logger.info(f"Found {len(missing_daily_dates)} missing daily files in the past 30 days")
            else:
                logger.info("No missing daily files in the past 30 days")
                
        except Exception as e:
            logger.error(f"Error checking for missing daily files: {e}")
    
    # NEW: Check for missing monthly files in the past 6 months
    logger.info("Checking for missing monthly files in the past 6 months...")
    missing_monthly_dates = []
    
    if drive:
        try:
            # Get all files currently in the monthly folder
            monthly_files = drive.ListFile({"q": f"'{MONTHLY_DRIVE_FOLDER_ID}' in parents and trashed=false"}).GetList()
            monthly_file_names = {file["title"] for file in monthly_files}
            
            logger.info(f"Found {len(monthly_file_names)} files in monthly folder")
            
            # Check each of the past 6 months
            for months_ago in range(1, 7):  # 1 to 6 months ago
                # Get the first day of the month X months ago
                check_date = (today.replace(day=1) - timedelta(days=1))  # Last month's last day
                for _ in range(months_ago - 1):
                    check_date = (check_date.replace(day=1) - timedelta(days=1))
                
                check_month_start = check_date.replace(day=1)
                expected_filename = f"CONUS{check_month_start.strftime('%Y_%m')}.zip"
                
                if expected_filename not in monthly_file_names:
                    missing_monthly_dates.append(check_month_start)
                    logger.info(f"Missing monthly file: {expected_filename}")
            
            if missing_monthly_dates:
                logger.info(f"Found {len(missing_monthly_dates)} missing monthly files in the past 6 months")
            else:
                logger.info("No missing monthly files in the past 6 months")
                
        except Exception as e:
            logger.error(f"Error checking for missing monthly files: {e}")
    
    # THIRD: Process yesterday's data (normal daily processing)
    logger.info(f"Processing yesterday's data: {yesterday_str}")
    yesterday_success = process_one_day(yesterday_str, fxx, product, regex, state, drive, hp)
    
    if yesterday_success:
        logger.info(f"Successfully processed daily data for {yesterday_str}")
    else:
        logger.error(f"Failed to process daily data for {yesterday_str}")
    
    cleanup_grib()
    
    # FOURTH: Process missing daily files from the past 30 days
    if missing_daily_dates:
        logger.info(f"Processing {len(missing_daily_dates)} missing daily files...")
        successful = 0
        failed = 0
        
        for i, missing_date in enumerate(missing_daily_dates, 1):
            logger.info(f"Processing missing daily file {i}/{len(missing_daily_dates)}: {missing_date.strftime('%Y-%m-%d')}")
            
            success = process_one_day(missing_date, fxx, product, regex, state, drive, hp)
            if success:
                successful += 1
            else:
                failed += 1
            
            cleanup_grib()
        
        logger.info(f"Missing daily files summary: {successful} successful, {failed} failed")
    
    # FIFTH: Process missing monthly files from the past 6 months
    if missing_monthly_dates:
        logger.info(f"Processing {len(missing_monthly_dates)} missing monthly files...")
        successful = 0
        failed = 0
        
        for i, missing_month in enumerate(missing_monthly_dates, 1):
            logger.info(f"Processing missing monthly file {i}/{len(missing_monthly_dates)}: {missing_month.strftime('%Y-%m')}")
            
            success = process_one_month(missing_month, fxx, product, regex, state, drive, hp)
            if success:
                successful += 1
            else:
                failed += 1
            
            cleanup_grib()
        
        logger.info(f"Missing monthly files summary: {successful} successful, {failed} failed")
    
    # SIXTH: Check if today is the first day of the month for monthly processing
    if today.day == 1:
        logger.info(f"First day of month detected! Processing entire previous month.")
        
        # Process the entire previous month
        last_month = today.replace(day=1) - timedelta(days=1)  # Go to last day of previous month
        last_month_start = last_month.replace(day=1)  # First day of that month
        
        last_month_str = last_month_start.strftime("%Y-%m")
        logger.info(f"Processing entire previous month: {last_month_str}")
        
        monthly_success = process_one_month(last_month_start, fxx, product, regex, state, drive, hp)
        
        if monthly_success:
            logger.info(f"Successfully processed monthly data for {last_month_str}")
        else:
            logger.error(f"Failed to process monthly data for {last_month_str}")
    
    logger.info("Historical processing complete!")
    return True


if __name__ == "__main__":
    import sys
    
    # Check if manual mode is requested
    if len(sys.argv) > 1 and sys.argv[1] == "--manual":
        manual_processing()
    else:
        # Default: process based on current date (for crontab)
        main()