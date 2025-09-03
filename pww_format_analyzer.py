#!/usr/bin/env python3
"""
Analyze the exact PWW/PowerWorld station format structure
"""

import struct

def analyze_exact_structure():
    """Parse the exact 35-byte records properly"""
    
    with open("era5_station.pkl", 'rb') as f:
        raw_data = f.read()
    
    print("Analyzing exact 35-byte structure:")
    print("="*50)
    
    record_size = 35
    num_records = len(raw_data) // record_size
    
    print(f"File size: {len(raw_data)} bytes")
    print(f"Record size: {record_size} bytes") 
    print(f"Number of complete records: {num_records}")
    print(f"Remaining bytes: {len(raw_data) % record_size}")
    
    # Analyze first 10 records byte by byte
    for i in range(min(10, num_records)):
        start = i * record_size
        end = start + record_size
        record = raw_data[start:end]
        
        print(f"\nRecord {i}:")
        print(f"  Hex: {record.hex()}")
        print(f"  Raw: {record}")
        
        # Try different interpretations of the 35 bytes
        
        # Try: 3 bytes header + 8 lat + 8 lon + 16 string
        try:
            header = record[:3]
            lat_bytes = record[3:11]
            lon_bytes = record[11:19] 
            string_part = record[19:]
            
            lat = struct.unpack('<d', lat_bytes)[0]
            lon = struct.unpack('<d', lon_bytes)[0]
            string_readable = string_part.decode('ascii', errors='ignore').rstrip('\x00')
            
            print(f"  Interpretation 1:")
            print(f"    Header: {header.hex()}")
            print(f"    Lat: {lat:.6f}")
            print(f"    Lon: {lon:.6f}")  
            print(f"    String: '{string_readable}'")
            
        except Exception as e:
            print(f"  Interpretation 1 failed: {e}")
        
        # Try: 16 bytes (2 doubles) + 19 bytes string  
        try:
            coords_bytes = record[:16]
            string_part = record[16:]
            
            lat, lon = struct.unpack('<dd', coords_bytes)
            string_readable = string_part.decode('ascii', errors='ignore').rstrip('\x00')
            
            print(f"  Interpretation 2:")
            print(f"    Lat: {lat:.6f}")
            print(f"    Lon: {lon:.6f}")
            print(f"    String: '{string_readable}'")
            
        except Exception as e:
            print(f"  Interpretation 2 failed: {e}")
            
        # Try to find where the actual coordinate string starts
        coord_start = record.find(b'24.00')
        if coord_start != -1:
            print(f"  Coordinate string starts at byte {coord_start}")
            before_string = record[:coord_start]
            print(f"  Bytes before string: {before_string.hex()}")
            
            # Try parsing the bytes before the string as doubles
            if len(before_string) >= 16:
                try:
                    # Try last 16 bytes before string as lat/lon
                    coord_doubles = before_string[-16:]
                    lat, lon = struct.unpack('<dd', coord_doubles)
                    print(f"  Last 16 bytes as doubles: lat={lat:.6f}, lon={lon:.6f}")
                except:
                    pass
                    
                try:
                    # Try different positions
                    for offset in range(0, len(before_string)-15, 1):
                        test_bytes = before_string[offset:offset+16]
                        if len(test_bytes) == 16:
                            lat, lon = struct.unpack('<dd', test_bytes)
                            if 20 <= lat <= 30 and -170 <= lon <= -150:  # Hawaii range
                                print(f"  Offset {offset}: lat={lat:.6f}, lon={lon:.6f} ✓")
                                break
                except:
                    pass

def generate_pww_replacement():
    """Generate replacement code for PWW format"""
    
    print(f"\n" + "="*60)
    print("PWW-SPECIFIC REPLACEMENT CODE:")
    print("="*60)
    
    code = '''
def create_pww_station_data(lats, lons):
    """
    Create PowerWorld PWW format station data
    Based on analysis of era5_station.pkl structure
    """
    station_data = []
    
    for lat in lats:
        for lon in lons:
            # Create 35-byte record for PWW format
            record = bytearray(35)
            
            # Fill with the pattern observed in era5_station.pkl
            # (This needs to be adjusted based on the analysis above)
            
            # Method 1: If coordinates are at specific positions
            lat_bytes = struct.pack('<d', float(lat))
            lon_bytes = struct.pack('<d', float(lon))
            
            # Position them based on analysis results
            record[3:11] = lat_bytes    # Adjust offsets based on analysis
            record[11:19] = lon_bytes   # Adjust offsets based on analysis
            
            # Add coordinate string
            coord_str = f"{lat:.2f}{lon:.2f}/" if lon < 0 else f"{lat:.2f}+{lon:.2f}/"
            coord_bytes = coord_str.encode('ascii')
            string_start = 19  # Adjust based on analysis
            record[string_start:string_start+len(coord_bytes)] = coord_bytes
            
            station_data.append(bytes(record))
    
    return b''.join(station_data)

# Usage in NCtoPWW:
# BYTECOUNT = 35  # PowerWorld expects 35 bytes per station
# sta = create_pww_station_data(df.latitude.values, df.longitude.values)
'''
    
    print(code)

if __name__ == "__main__":
    analyze_exact_structure()
    generate_pww_replacement()