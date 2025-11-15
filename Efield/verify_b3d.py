#!/usr/bin/env python3
"""
Simple script to verify B3D files
"""
from b3d import B3D
import numpy as np
import matplotlib.pyplot as plt

def verify_b3d_file(filepath):
    """Verify and display info about a B3D file"""
    print(f"\n=== Verifying {filepath} ===")
    
    try:
        # Load the B3D file
        b3d = B3D(filepath)
        
        # Basic info
        print(f"Comment: {b3d.comment}")
        print(f"Time units: {b3d.time_units}")
        print(f"Time 0: {b3d.time_0}")
        print(f"Grid dimensions: {b3d.grid_dim}")
        
        # Data dimensions
        print(f"Number of stations: {len(b3d.lat)}")
        print(f"Number of time points: {len(b3d.time)}")
        print(f"Ex data shape: {b3d.ex.shape}")
        print(f"Ey data shape: {b3d.ey.shape}")
        
        # Coordinate ranges
        print(f"Latitude range: {b3d.lat.min():.3f} to {b3d.lat.max():.3f}")
        print(f"Longitude range: {b3d.lon.min():.3f} to {b3d.lon.max():.3f}")
        
        # Data ranges (excluding NaN values)
        ex_valid = b3d.ex[~np.isnan(b3d.ex)]
        ey_valid = b3d.ey[~np.isnan(b3d.ey)]
        
        if len(ex_valid) > 0:
            print(f"Ex range: {ex_valid.min():.6f} to {ex_valid.max():.6f}")
            print(f"Ex mean: {ex_valid.mean():.6f}")
        
        if len(ey_valid) > 0:
            print(f"Ey range: {ey_valid.min():.6f} to {ey_valid.max():.6f}")
            print(f"Ey mean: {ey_valid.mean():.6f}")
        
        # Check for data quality
        nan_count_ex = np.isnan(b3d.ex).sum()
        nan_count_ey = np.isnan(b3d.ey).sum()
        total_points = b3d.ex.size
        
        print(f"Ex NaN values: {nan_count_ex}/{total_points} ({100*nan_count_ex/total_points:.1f}%)")
        print(f"Ey NaN values: {nan_count_ey}/{total_points} ({100*nan_count_ey/total_points:.1f}%)")
        
        print("✅ File loaded successfully!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return False

def plot_b3d_sample(filepath, save_plot=True):
    """Create a simple plot of the B3D data"""
    try:
        b3d = B3D(filepath)
        
        # Create a simple plot
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
        
        # Plot first time step
        if len(b3d.time) > 0:
            # Get data for first time step
            ex_first = b3d.ex[0, :]
            ey_first = b3d.ey[0, :]
            
            # Scatter plot of Ex values
            scatter1 = ax1.scatter(b3d.lon, b3d.lat, c=ex_first, cmap='RdBu_r', s=10)
            ax1.set_title(f'Ex Field (t=0)')
            ax1.set_xlabel('Longitude')
            ax1.set_ylabel('Latitude')
            plt.colorbar(scatter1, ax=ax1)
            
            # Scatter plot of Ey values  
            scatter2 = ax2.scatter(b3d.lon, b3d.lat, c=ey_first, cmap='RdBu_r', s=10)
            ax2.set_title(f'Ey Field (t=0)')
            ax2.set_xlabel('Longitude')
            ax2.set_ylabel('Latitude')
            plt.colorbar(scatter2, ax=ax2)
            
            plt.tight_layout()
            
            if save_plot:
                plot_filename = filepath.replace('.b3d', '_plot.png')
                plt.savefig(plot_filename, dpi=150, bbox_inches='tight')
                print(f"Plot saved as: {plot_filename}")
            
            plt.show()
        
    except Exception as e:
        print(f"Error creating plot: {e}")

if __name__ == "__main__":
    # Verify the files created today
    files_to_check = [
        # "Data/1D/20250720_1D.b3d",
        # "Data/1D/20250331_1D.b3d",
        "Data/3D/20241204_3D.b3d",


    ]
    
    for filepath in files_to_check:
        if verify_b3d_file(filepath):
            # Create a plot if verification successful
            plot_b3d_sample(filepath)