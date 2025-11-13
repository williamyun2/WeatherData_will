# Pack custom date range into a single PWW file
# Place this in the same directory as your cds_auto.py

import os
import re
import struct
from glob import glob
from datetime import datetime
import xarray as xr
import pandas as pd
import numpy as np

# Import from your existing script
from cds_auto import Data, NCtoPWW

def pack_custom_range(start_date, end_date, region_name="hawaii"):
    """
    Pack all NC files within a date range into a single PWW file
    
    Args:
        start_date: datetime object for start date
        end_date: datetime object for end date
        region_name: name of the region (default: "hawaii")
    """
    
    print(f"📦 Packing data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Find all NC files in the date range
    date_pattern = re.compile(r"(\d{8})")
    files = glob(rf"{Data}/nc/*")
    matched_files = []
    
    for file in files:
        match = date_pattern.search(file)
        if match:
            file_date = datetime.strptime(match.group(1), "%Y%m%d")
            if start_date <= file_date <= end_date:
                matched_files.append(file)
    
    print(f"✅ Found {len(matched_files)} NC files in date range")
    
    if len(matched_files) == 0:
        print("❌ No files found in the specified date range!")
        return
    
    # Load and merge all NC files
    print("🔄 Loading and merging NetCDF files...")
    ncs = []
    actual_dates = []
    
    for nc_path in matched_files:
        try:
            ds_acc = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-accum.nc")
            ds = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-instant.nc")
            ds_gust = xr.open_dataset(f"{nc_path}/data_stream-oper_stepType-max.nc")
            ncs.append(xr.merge([ds, ds_acc, ds_gust]))
            
            match = date_pattern.search(nc_path)
            if match:
                actual_dates.append(datetime.strptime(match.group(1), "%Y%m%d"))
        except Exception as e:
            print(f"⚠️  Error loading {nc_path}: {e}")
            continue
    
    if len(ncs) == 0:
        print("❌ No valid NC files could be loaded!")
        return
    
    # Concatenate all datasets
    print("🔗 Concatenating datasets...")
    ds = xr.concat(ncs, dim="valid_time")
    ds = ds.dropna("valid_time", how="all")
    ds = ds.drop_duplicates("valid_time")
    ds = ds.sortby("valid_time")
    
    # Generate output filename
    actual_start = min(actual_dates)
    actual_end = max(actual_dates)
    file_name = f"{region_name.capitalize()}_{actual_start.strftime('%Y-%m-%d')}_to_{actual_end.strftime('%Y-%m-%d')}"
    
    print(f"📝 Creating PWW file: {file_name}.pww")
    print(f"📊 Total time steps: {len(ds.valid_time)}")
    
    # Create the PWW file
    output_path = f"{Data}/pww/custom/{file_name}.pww"
    os.makedirs(f"{Data}/pww/custom", exist_ok=True)
    
    NCtoPWW(ds, output_path, region_name)
    
    print(f"✅ Successfully created: {output_path}")
    print(f"📈 Date range: {actual_start.strftime('%Y-%m-%d')} to {actual_end.strftime('%Y-%m-%d')}")
    print(f"⏱️  Total hours of data: {len(ds.valid_time)}")

if __name__ == "__main__":
    # Configuration
    START_DATE = datetime(2020, 1, 1)
    END_DATE = datetime(2025, 9, 30)
    REGION_NAME = "hawaii"
    
    # Pack the data
    pack_custom_range(START_DATE, END_DATE, REGION_NAME)