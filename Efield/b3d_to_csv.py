#!/usr/bin/env python3
"""
Convert B3D files to CSV format for viewing in Excel or other tools
"""
from b3d import B3D
import pandas as pd
import numpy as np

def b3d_to_csv(b3d_filepath, output_csv=None):
    """Convert B3D file to CSV format"""
    if output_csv is None:
        output_csv = b3d_filepath.replace('.b3d', '.csv')
    
    # Load B3D file
    b3d = B3D(b3d_filepath)
    
    # Create a list to store all data
    data_rows = []
    
    # For each time step
    for t_idx, time_val in enumerate(b3d.time):
        # For each station
        for s_idx in range(len(b3d.lat)):
            row = {
                'time_index': t_idx,
                'time_seconds': time_val,
                'latitude': b3d.lat[s_idx],
                'longitude': b3d.lon[s_idx],
                'Ex': b3d.ex[t_idx, s_idx],
                'Ey': b3d.ey[t_idx, s_idx],
                'nearest_station': b3d.n_station[s_idx] if hasattr(b3d, 'n_station') else -1
            }
            data_rows.append(row)
    
    # Create DataFrame and save
    df = pd.DataFrame(data_rows)
    df.to_csv(output_csv, index=False)
    print(f"Converted {b3d_filepath} -> {output_csv}")
    print(f"CSV contains {len(df)} rows")
    
    return output_csv

def b3d_to_summary_csv(b3d_filepath, output_csv=None):
    """Create a summary CSV with station info"""
    if output_csv is None:
        output_csv = b3d_filepath.replace('.b3d', '_summary.csv')
    
    b3d = B3D(b3d_filepath)
    
    # Create summary data for each station
    summary_data = []
    for s_idx in range(len(b3d.lat)):
        # Calculate statistics for this station across all time steps
        ex_vals = b3d.ex[:, s_idx]
        ey_vals = b3d.ey[:, s_idx]
        
        # Remove NaN values for statistics
        ex_valid = ex_vals[~np.isnan(ex_vals)]
        ey_valid = ey_vals[~np.isnan(ey_vals)]
        
        row = {
            'station_index': s_idx,
            'latitude': b3d.lat[s_idx],
            'longitude': b3d.lon[s_idx],
            'nearest_station': b3d.n_station[s_idx] if hasattr(b3d, 'n_station') else -1,
            'ex_mean': ex_valid.mean() if len(ex_valid) > 0 else np.nan,
            'ex_std': ex_valid.std() if len(ex_valid) > 0 else np.nan,
            'ex_min': ex_valid.min() if len(ex_valid) > 0 else np.nan,
            'ex_max': ex_valid.max() if len(ex_valid) > 0 else np.nan,
            'ey_mean': ey_valid.mean() if len(ey_valid) > 0 else np.nan,
            'ey_std': ey_valid.std() if len(ey_valid) > 0 else np.nan,
            'ey_min': ey_valid.min() if len(ey_valid) > 0 else np.nan,
            'ey_max': ey_valid.max() if len(ey_valid) > 0 else np.nan,
            'valid_time_points': len(ex_valid)
        }
        summary_data.append(row)
    
    df = pd.DataFrame(summary_data)
    df.to_csv(output_csv, index=False)
    print(f"Summary saved to {output_csv}")
    
    return output_csv

if __name__ == "__main__":
    # Convert your B3D files to CSV
    b3d_files = [
        "Data/1D/20250720_1D.b3d",
        "Data/1D/20250331_1D.b3d",
        "Data/3D/20250720_3D.b3d"
    ]
    
    for b3d_file in b3d_files:
        # Full data CSV (might be large!)
        # b3d_to_csv(b3d_file)
        
        # Summary CSV (smaller, good for overview)
        b3d_to_summary_csv(b3d_file)