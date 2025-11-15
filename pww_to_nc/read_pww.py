from PWW_to_NC import PWW_to_NC
import os

# Specify the quarterly PWW file
pww_file = r"C:\class\code\service_testing\cds\data\pww\custom\Hawaii_2020-01-01_to_2025-09-30.pww"

# Check if file exists
if not os.path.exists(pww_file):
    print(f"Error: File not found: {pww_file}")
else:
    print(f"Found file: {os.path.basename(pww_file)}")
    print(f"File size: {os.path.getsize(pww_file) / (1024*1024):.2f} MB")
    
    # Convert the PWW file
    print(f"\nConverting: {pww_file}")
    ds = PWW_to_NC(pww_file)
    
    print("\nDataset structure:")
    print(ds)
    print(f"\nTime range: {ds.time.min().values} to {ds.time.max().values}")
    print(f"Number of time steps: {len(ds.time)}")
    print(f"Spatial dimensions: {ds.dims['lat']} x {ds.dims['lon']}")
    
    # Save to NetCDF in the current directory
    output_filename = os.path.basename(pww_file).replace('.pww', '.nc')
    output_file = os.path.join(os.getcwd(), output_filename)
    print(f"\nSaving to: {output_file}")
    ds.to_netcdf(output_file, engine='h5netcdf')
    print(f"Success! Saved NetCDF file ({os.path.getsize(output_file) / (1024*1024):.2f} MB)")