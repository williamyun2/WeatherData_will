#!/usr/bin/env python3
"""
Script to process historical electric field data for a specific date range
Run this to process and upload data for dates 8/12/25 to 8/22/25
"""

import datetime
from Efield import process_data, upload_to_drive, check
from google.oauth2 import service_account
from googleapiclient.discovery import build

def process_date_range(start_date, end_date):
    """
    Process data for a range of dates
    
    Args:
        start_date (str): Start date in format 'YYYYMMDD' (e.g., '20250812')
        end_date (str): End date in format 'YYYYMMDD' (e.g., '20250822')
    """
    print(f"Processing data from {start_date} to {end_date}")
    
    # Setup Google Drive API
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = 'pydrive2-461721-19b0d14ec905.json'
    
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    
    # Convert string dates to datetime objects
    start_dt = datetime.datetime.strptime(start_date, '%Y%m%d')
    end_dt = datetime.datetime.strptime(end_date, '%Y%m%d')
    
    current_date = start_dt
    processed_dates = []
    
    while current_date <= end_dt:
        date_str = current_date.strftime('%Y%m%d')
        print(f"\n{'='*50}")
        print(f"Processing date: {date_str}")
        print(f"{'='*50}")
        
        try:
            # Process 1D data
            print("Processing 1D data...")
            num_1d = process_data("1D", date_str)
            print(f"Processed {num_1d} 1D files")
            
            # Process 3D data  
            print("Processing 3D data...")
            num_3d = process_data("3D", date_str)
            print(f"Processed {num_3d} 3D files")
            
            if num_1d > 0 or num_3d > 0:
                processed_dates.append(date_str)
                print(f"✅ Successfully processed data for {date_str}")
            else:
                print(f"⚠️  No data found for {date_str}")
                
        except Exception as e:
            print(f"❌ Error processing {date_str}: {e}")
            import traceback
            traceback.print_exc()
        
        # Move to next day
        current_date += datetime.timedelta(days=1)
    
    print(f"\n{'='*60}")
    print("PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Processed dates: {len(processed_dates)}")
    for date in processed_dates:
        print(f"  ✅ {date}")
    
    # Upload all processed files
    if processed_dates:
        print(f"\n{'='*60}")
        print("STARTING UPLOADS")
        print(f"{'='*60}")
        
        try:
            print("Uploading 1D files...")
            # Production folders
            upload_to_drive(drive_service, "1pItMc-ViWiRbY6G49sLlmP_0B5fRMh1W", "data/1D/*b3d")
            
            print("Uploading 3D files...")
            upload_to_drive(drive_service, "1JIOe_ANudOk2zW9v9LpSJ9UeKX-Zo4Ch", "data/3D/*b3d")
            
            print("✅ Upload completed!")
            
        except Exception as e:
            print(f"❌ Upload error: {e}")
            import traceback
            traceback.print_exc()
    
    return processed_dates

def main():
    """Main function to run the date range processing"""
    # Ensure data directories exist
    check()
    
    # Define date range (August 12, 2025 to August 22, 2025)
    start_date = "20250812"  # August 12, 2025
    end_date = "20250822"    # August 22, 2025
    
    print("Historical Data Processing Script")
    print("=" * 50)
    print(f"Date range: {start_date} to {end_date}")
    print("=" * 50)
    
    processed = process_date_range(start_date, end_date)
    
    print(f"\n🎉 Script completed!")
    print(f"📊 Total dates processed: {len(processed)}")

if __name__ == "__main__":
    main()


    # https://claude.ai/chat/73d9e61c-59c7-424c-9295-61ef5d7e811b