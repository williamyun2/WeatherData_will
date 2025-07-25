#!/usr/bin/env python3
"""
Setup and test script for Google Service Account authentication
Run this to verify your service account is working correctly
"""

import os
import sys
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SERVICE_ACCOUNT_FILE = 'pydrive2-461721-19b0d14ec905.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

# Production folder IDs
PRODUCTION_FOLDERS = {
    'daily': "1dmXrU8qtkMkPbQl6QxNToZBUmjIORIxe",
    'daily_archive': "1EepB8GlTLqOl5iSgXz0WEINw6lcjyuaa", 
    'quarterly': "1h4TeCcAc0khTkeGFtSNubwgFsY5CD8pH"
}

def test_service_account():
    """Test if service account can authenticate"""
    print("\n=== Testing Service Account Authentication ===")
    
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"❌ Service account file not found: {SERVICE_ACCOUNT_FILE}")
        print("\nPlease ensure you have the service account JSON file in the current directory.")
        print("The file should be named: pydrive2-461721-19b0d14ec905.json")
        return None
    
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=creds)
        
        # Test API access
        about = service.about().get(fields="user").execute()
        print(f"✅ Successfully authenticated as: {about.get('user', {}).get('emailAddress', 'Unknown')}")
        
        return service
    
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return None

def test_folder_access(service, folder_id, folder_name):
    """Test if service account can access a specific folder"""
    try:
        # Try to list files in the folder
        results = service.files().list(
            q=f"'{folder_id}' in parents",
            pageSize=5,
            fields="files(id, name)"
        ).execute()
        
        files = results.get('files', [])
        print(f"✅ Can access {folder_name} folder (ID: {folder_id})")
        print(f"   Found {len(files)} files")
        
        if files:
            print("   Sample files:")
            for file in files[:3]:
                print(f"   - {file['name']}")
        
        return True
        
    except HttpError as error:
        if error.resp.status == 404:
            print(f"❌ {folder_name} folder not found or no access (ID: {folder_id})")
            print("   Make sure the folder is shared with the service account email")
        else:
            print(f"❌ Error accessing {folder_name} folder: {error}")
        return False

def test_upload_download(service):
    """Test file upload and download capabilities"""
    print("\n=== Testing Upload/Download Capabilities ===")
    
    # Create a test file
    test_filename = "test_era5_upload.txt"
    test_content = "This is a test file for ERA5 service account"
    
    try:
        # Create test file locally
        with open(test_filename, 'w') as f:
            f.write(test_content)
        
        print(f"✅ Created local test file: {test_filename}")
        
        # Note: Actual upload test would require a writable test folder
        print("ℹ️  Upload test requires a dedicated test folder with write permissions")
        
        # Clean up
        os.remove(test_filename)
        print(f"✅ Cleaned up local test file")
        
    except Exception as e:
        print(f"❌ Error in upload/download test: {e}")

def create_test_folders(service):
    """Offer to create test folders"""
    print("\n=== Test Folder Creation ===")
    response = input("Would you like to create test folders? (y/n): ").lower()
    
    if response != 'y':
        print("Skipping test folder creation")
        return None
    
    try:
        # Import helper
        from helper import GoogleDriveHelper, setup_test_folders
        
        folders = setup_test_folders(service, logger)
        if folders:
            print("\n✅ Test folders created successfully!")
            print("\nAdd these to your main script for testing mode:")
            print(f"DAILY_FOLDER_ID = '{folders['daily']}'")
            print(f"DAILY_ARCHIVE_FOLDER_ID = '{folders['archive']}'")
            print(f"QUARTERLY_FOLDER_ID = '{folders['quarterly']}'")
            return folders
    
    except ImportError:
        print("❌ Could not import helper module. Make sure helper.py is in the helper/ directory")
    except Exception as e:
        print(f"❌ Error creating test folders: {e}")
    
    return None

def main():
    """Main test routine"""
    print("ERA5 Google Service Account Setup Test")
    print("=" * 50)
    
    # Test authentication
    service = test_service_account()
    if not service:
        print("\n❌ Setup failed. Please check your service account file.")
        sys.exit(1)
    
    # Test folder access
    print("\n=== Testing Production Folder Access ===")
    all_accessible = True
    
    for folder_type, folder_id in PRODUCTION_FOLDERS.items():
        accessible = test_folder_access(service, folder_id, folder_type)
        if not accessible:
            all_accessible = False
    
    if not all_accessible:
        print("\n⚠️  Some folders are not accessible.")
        print("\nTo fix this:")
        print("1. Go to each Google Drive folder in your browser")
        print("2. Right-click and select 'Share'")
        print("3. Add the service account email (shown above) with 'Editor' permissions")
        print("4. Make sure 'Notify people' is unchecked")
        print("5. Click 'Share'")
    
    # Test upload/download
    test_upload_download(service)
    
    # Offer to create test folders
    create_test_folders(service)
    
    # Summary
    print("\n=== Setup Summary ===")
    if all_accessible:
        print("✅ All production folders are accessible")
        print("✅ Service account is properly configured")
        print("\nYou can now run the main ERA5 script!")
    else:
        print("⚠️  Setup is incomplete. Please fix the issues above.")
    
    print("\n=== Configuration Details ===")
    print(f"Service Account File: {SERVICE_ACCOUNT_FILE}")
    print(f"Testing Mode: Set TESTING_MODE = True in main script for testing")
    print(f"Production Mode: Set TESTING_MODE = False for production use")

if __name__ == "__main__":
    main()