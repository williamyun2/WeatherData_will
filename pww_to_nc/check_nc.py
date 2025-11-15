#!/usr/bin/env python3
"""
Verify solar irradiance physics: DHI must always be ≤ GHI
"""

import xarray as xr
import numpy as np
import sys

# Load the NetCDF file
nc_file = "Hawaii_2020-01-01_to_2025-09-30.nc"
if len(sys.argv) > 1:
    nc_file = sys.argv[1]

print(f"📂 Loading: {nc_file}\n")
ds = xr.open_dataset(nc_file)

# Get the irradiance data
ghi = ds['global_horizontal_irradiance_wm2'].values
dhi = ds['direct_horizontal_irradiance_wm2'].values

print("="*70)
print("🌞 SOLAR IRRADIANCE PHYSICS CHECK")
print("="*70)

print("\n📊 Overall Statistics:")
print(f"GHI: Min={np.min(ghi):.1f}, Max={np.max(ghi):.1f}, Mean={np.mean(ghi):.1f} W/m²")
print(f"DHI: Min={np.min(dhi):.1f}, Max={np.max(dhi):.1f}, Mean={np.mean(dhi):.1f} W/m²")

# Check for violations: DHI > GHI
violations = dhi > ghi
num_violations = np.sum(violations)
total_points = ghi.size

print(f"\n🔍 Physics Check: DHI ≤ GHI")
print(f"Total data points: {total_points:,}")
print(f"Violations (DHI > GHI): {num_violations:,} ({num_violations/total_points*100:.4f}%)")

if num_violations > 0:
    print(f"\n⚠️  WARNING: Found {num_violations:,} cases where DHI > GHI!")
    print("This violates fundamental solar physics!")
    
    # Find worst violations
    diff = dhi - ghi
    worst_idx = np.unravel_index(np.argmax(diff), diff.shape)
    
    print(f"\nWorst violation:")
    print(f"  Time index: {worst_idx[0]}")
    print(f"  Location: lat={worst_idx[1]}, lon={worst_idx[2]}")
    print(f"  GHI: {ghi[worst_idx]:.1f} W/m²")
    print(f"  DHI: {dhi[worst_idx]:.1f} W/m²")
    print(f"  Difference: {diff[worst_idx]:.1f} W/m²")
    
    # Show distribution of violations
    violation_diffs = diff[violations]
    print(f"\nViolation magnitude:")
    print(f"  Mean excess: {np.mean(violation_diffs):.1f} W/m²")
    print(f"  Max excess: {np.max(violation_diffs):.1f} W/m²")
    
else:
    print("\n✅ PASS: All DHI values are ≤ GHI (physics check passed)")

# Check the difference distribution
print("\n📈 Difference Distribution (GHI - DHI):")
diff_valid = ghi - dhi
print(f"  Min: {np.min(diff_valid):.1f} W/m²")
print(f"  Max: {np.max(diff_valid):.1f} W/m²")
print(f"  Mean: {np.mean(diff_valid):.1f} W/m²")
print(f"  Median: {np.median(diff_valid):.1f} W/m²")

# Check for suspicious patterns
print("\n🔍 Additional Checks:")

# Check if DHI = GHI (overcast conditions)
equal_count = np.sum(np.abs(ghi - dhi) < 1)  # Within 1 W/m²
print(f"  Cases where DHI ≈ GHI (overcast): {equal_count:,} ({equal_count/total_points*100:.2f}%)")

# Check if DHI = 0 when GHI > 0 (suspicious)
dhi_zero_ghi_nonzero = np.sum((dhi == 0) & (ghi > 10))
print(f"  Cases where DHI=0 but GHI>10: {dhi_zero_ghi_nonzero:,} ({dhi_zero_ghi_nonzero/total_points*100:.2f}%)")

# Check nighttime (both should be 0)
nighttime = np.sum((ghi < 1) & (dhi < 1))
print(f"  Nighttime points (both ≈0): {nighttime:,} ({nighttime/total_points*100:.2f}%)")

# Typical DHI/GHI ratio
daytime_mask = ghi > 50
if np.sum(daytime_mask) > 0:
    ghi_day = ghi[daytime_mask]
    dhi_day = dhi[daytime_mask]
    ratio = dhi_day / ghi_day
    print(f"\n☀️  Daytime DHI/GHI Ratio (when GHI > 50 W/m²):")
    print(f"  Mean ratio: {np.mean(ratio):.2f}")
    print(f"  Median ratio: {np.median(ratio):.2f}")
    print(f"  Expected: 0.1-0.3 for clear skies, 0.7-1.0 for overcast")

print("\n" + "="*70)

ds.close()


# https://claude.ai/chat/61127697-eeda-4edb-86c6-6858398c303d
# GHI and GHI naming issue.