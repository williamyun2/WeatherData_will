#!/usr/bin/env python3
"""
Detailed scanner for era5_station.pkl based on the observed pattern
"""

import struct
import re

def analyze_station_structure(pkl_path="era5_station.pkl"):
    """Analyze the specific structure of era5_station.pkl"""
    
    with open(pkl_path, 'rb') as f:
        raw_data = f.read()
    
    print(f"File size: {len(raw_data)} bytes")
    
    # Look for the coordinate pattern
    coord_pattern = rb'[0-9]+\.[0-9]+-[0-9]+\.[0-9]+/'
    matches = list(re.finditer(coord_pattern, raw_data))
    
    print(f"Found {len(matches)} coordinate patterns")
    
    if matches:
        print("\nFirst 10 coordinate strings:")
        for i, match in enumerate(matches[:10]):
            coord_str = match.group().decode('ascii')
            print(f"  {i}: {coord_str}")
        
        # Calculate bytes between coordinates
        if len(matches) > 1:
            gap = matches[1].start() - matches[0].end()
            print(f"\nBytes between coordinate strings: {gap}")
        
        # Estimate record size
        first_match_start = matches[0].start()
        print(f"Bytes before first coordinate: {first_match_start}")
        
        # Try to determine record structure
        if len(matches) > 1:
            record_size = matches[1].start() - matches[0].start()
            print(f"Estimated bytes per record: {record_size}")
            
            estimated_records = len(raw_data) // record_size
            print(f"Estimated number of records: {estimated_records}")
    
    # Examine the structure around coordinates
    print(f"\nDetailed structure analysis:")
    
    # Look at first few records in detail
    pos = 0
    for i in range(min(5, len(matches))):
        match = matches[i]
        record_start = match.start() - 16 if match.start() >= 16 else 0  # Go back 16 bytes
        record_end = match.end() + 16 if match.end() + 16 < len(raw_data) else len(raw_data)
        
        record_data = raw_data[record_start:record_end]
        
        print(f"\nRecord {i} (around position {match.start()}):")
        print(f"  Hex: {record_data.hex()}")
        print(f"  Raw: {record_data}")
        
        # Try to parse as doubles before the coordinate string
        try:
            # Get 16 bytes before the coordinate string
            if match.start() >= 16:
                before_coord = raw_data[match.start()-16:match.start()]
                lat, lon = struct.unpack('<dd', before_coord)
                print(f"  Doubles before coord: lat={lat:.6f}, lon={lon:.6f}")
                
                # Compare with coordinate string
                coord_str = match.group().decode('ascii').rstrip('/')
                if '+' in coord_str:
                    str_lat, str_lon = coord_str.split('+')
                else:
                    parts = coord_str.split('-')
                    if len(parts) == 3:  # negative longitude
                        str_lat = parts[0]
                        str_lon = '-' + parts[1]
                    else:
                        str_lat, str_lon = parts
                
                print(f"  String coord: lat={str_lat}, lon={str_lon}")
                
        except Exception as e:
            print(f"  Error parsing doubles: {e}")

def generate_correct_replacement():
    """Generate the correct replacement code based on analysis"""
    
    print(f"\n" + "="*60)
    print("CORRECTED REPLACEMENT CODE:")
    print("="*60)
    
    replacement = '''
def create_station_binary_data(lats, lons):
    """
    Create binary station data matching era5_station.pkl format
    Based on analysis: each record appears to contain:
    - 16 bytes: lat,lon as doubles
    - ~16+ bytes: coordinate string like "24.00-130.00/"
    """
    station_data = []
    
    for lat in lats:
        for lon in lons:
            # Pack latitude and longitude as doubles (16 bytes)
            binary_coords = struct.pack("<dd", float(lat), float(lon))
            
            # Create coordinate string (matching observed format)
            if lon < 0:
                coord_str = f"{lat:.2f}{lon:.2f}/"
            else:
                coord_str = f"{lat:.2f}+{lon:.2f}/"
            
            # Pad coordinate string to fixed length (observed ~16 bytes)
            coord_bytes = coord_str.encode('ascii').ljust(16, b'\\x00')
            
            # Combine binary coords + string coords
            station_data.append(binary_coords + coord_bytes)
    
    return b''.join(station_data)

# Usage in NCtoPWW:
# Replace: sta = open("era5_station.pkl", "rb").read()  
# With:    sta = create_station_binary_data(df.latitude.values, df.longitude.values)
'''
    
    print(replacement)

if __name__ == "__main__":
    analyze_station_structure()
    generate_correct_replacement()