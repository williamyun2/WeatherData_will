#!/usr/bin/env python3
"""
googdrivetest.py - Check Google Drive folders for duplicate dates

This script connects to Google Drive and analyzes files in the specified folders
to find duplicate dates and list all available dates.
"""

import os
import re
import pandas as pd
from collections import Counter
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def setup_drive_service():
    """Setup Google Drive API service"""
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = 'pydrive2-461721-19b0d14ec905.json'
    
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        drive_service = build('drive', 'v3', credentials=creds)
        print("✅ Google Drive API service initialized successfully")
        return drive_service
    except Exception as e:
        print(f"❌ Error setting up Google Drive service: {e}")
        return None

def get_drive_files(drive_service, folder_id, folder_name=""):
    """Get ALL files from a Google Drive folder (handles pagination)"""
    try:
        print(f"📂 Scanning folder {folder_name}...")
        all_files = []
        page_token = None
        page_count = 0
        
        while True:
            page_count += 1
            print(f"   📄 Loading page {page_count}...")
            
            response = drive_service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                fields="nextPageToken,files(id,name,createdTime,modifiedTime,size)",
                pageSize=1000,  # Maximum page size
                pageToken=page_token
            ).execute()
            
            files = response.get('files', [])
            all_files.extend(files)
            print(f"      Got {len(files)} files on this page")
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        
        print(f"   📊 Total files found: {len(all_files)}")
        return all_files
        
    except HttpError as error:
        print(f"❌ Error accessing folder {folder_name}: {error}")
        return []

def extract_dates_from_files(files):
    """Extract dates from filenames and organize file information"""
    dates = []
    file_info = []
    
    for file in files:
        filename = file['name']
        # Extract date from filename (looking for YYYYMMDD pattern)
        date_match = re.search(r'(\d{8})', filename)
        if date_match:
            date = date_match.group(1)
            dates.append(date)
            file_info.append({
                'filename': filename,
                'date': date,
                'file_id': file['id'],
                'created': file.get('createdTime', ''),
                'modified': file.get('modifiedTime', ''),
                'size': file.get('size', '')
            })
        else:
            # File doesn't have recognizable date format
            file_info.append({
                'filename': filename,
                'date': 'NO_DATE',
                'file_id': file['id'],
                'created': file.get('createdTime', ''),
                'modified': file.get('modifiedTime', ''),
                'size': file.get('size', '')
            })
    
    return dates, file_info

def find_duplicates(dates, file_info, folder_name):
    """Find and report duplicate dates"""
    print(f"\n{'='*50}")
    print(f"📊 ANALYSIS FOR {folder_name.upper()}")
    print(f"{'='*50}")
    
    # Count dates
    date_counts = Counter(dates)
    duplicates = {date: count for date, count in date_counts.items() if count > 1}
    
    # All unique dates (excluding NO_DATE)
    valid_dates = [d for d in dates if d != 'NO_DATE']
    unique_dates = sorted(set(valid_dates))
    
    print(f"📅 Total unique dates: {len(unique_dates)}")
    print(f"📁 Total files with dates: {len(valid_dates)}")
    
    if duplicates:
        print(f"\n🚨 DUPLICATE DATES FOUND: {len(duplicates)}")
        print("-" * 30)
        for date, count in sorted(duplicates.items()):
            print(f"📅 {date}: {count} files")
            # Show which files have this date
            duplicate_files = [f['filename'] for f in file_info if f['date'] == date]
            for filename in duplicate_files:
                print(f"    • {filename}")
            print()
    else:
        print("\n✅ No duplicate dates found!")
    
    # Show date range
    if unique_dates:
        print(f"\n📊 DATE RANGE:")
        print(f"   Earliest: {unique_dates[0]}")
        print(f"   Latest: {unique_dates[-1]}")
        
        # Show a sample of dates
        print(f"\n📋 SAMPLE DATES (first 10):")
        for date in unique_dates[:10]:
            print(f"   {date}")
        
        if len(unique_dates) > 10:
            print(f"   ... and {len(unique_dates) - 10} more dates")
    
    return unique_dates, duplicates

def main():
    """Main function to check both 1D and 3D folders"""
    print("🚀 Starting Google Drive duplicate date checker...")
    
    # Setup Drive service
    drive_service = setup_drive_service()
    if not drive_service:
        return
    
    # Folder IDs from your script
    folders = {
        "1D_PRODUCTION": "1pItMc-ViWiRbY6G49sLlmP_0B5fRMh1W",
        "3D_PRODUCTION": "1JIOe_ANudOk2zW9v9LpSJ9UeKX-Zo4Ch",
        # "1D_TEST": "1fnw5Olj7OOGbip19UTMgEktGUZ7dMapi",  # Uncomment to check test folders
        # "3D_TEST": "1emw7QyS1ICXBt8OhYBZ1GN82mDl36PpY"
    }
    
    all_results = {}
    
    for folder_name, folder_id in folders.items():
        # Get files from this folder
        files = get_drive_files(drive_service, folder_id, folder_name)
        
        if files:
            # Extract dates and analyze
            dates, file_info = extract_dates_from_files(files)
            unique_dates, duplicates = find_duplicates(dates, file_info, folder_name)
            
            all_results[folder_name] = {
                'files': files,
                'dates': dates,
                'unique_dates': unique_dates,
                'duplicates': duplicates,
                'file_info': file_info
            }
        else:
            print(f"⚠️ No files found in {folder_name}")
    
    # Summary across all folders
    print(f"\n{'='*60}")
    print("📈 OVERALL SUMMARY")
    print(f"{'='*60}")
    
    total_duplicates = 0
    for folder_name, results in all_results.items():
        dup_count = len(results['duplicates'])
        total_duplicates += dup_count
        print(f"{folder_name}: {len(results['unique_dates'])} unique dates, {dup_count} duplicate dates")
    
    print(f"\n🎯 TOTAL DUPLICATE DATES ACROSS ALL FOLDERS: {total_duplicates}")
    
    # Save detailed results to CSV for further analysis
    if all_results:
        print("\n💾 Saving detailed results to CSV...")
        all_file_info = []
        for folder_name, results in all_results.items():
            for file_info in results['file_info']:
                file_info['folder'] = folder_name
                all_file_info.append(file_info)
        
        df = pd.DataFrame(all_file_info)
        df.to_csv('drive_duplicates.csv', index=False)
        print("   Results saved to: drive_duplicates.csv")

if __name__ == "__main__":
    main()