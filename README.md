# CDS Auto Pipeline

## Quick Start

Run this:
```bash
python cds_auto.py
```

It does everything instead of `run1_download_nc.py`, `run2_nc_to_station.py`, `run3_station_pkl.py`, and `cds_history.py`.

## What It Does

- Downloads ERA5 weather data
- Automatically generates station data (parquet + pkl files)
- Converts to PWW format
- Handles all coordinate mapping

## Configuration

Edit the bottom of `cds_auto.py`:

```python
AREA = HAWAII                           # Change coordinates
START_DATE = datetime(2025, 8, 10)      # Change start date  
END_DATE = datetime(2025, 8, 17)        # Change end date
```

Predefined areas: `HAWAII`, `TEXAS`, `CALIFORNIA`, `FLORIDA`, `NORTHEAST`, `CONUS`

## Output Files

- `station/` - Station data files
- `data/pww/daily/` - Daily weather files  
- `data/pww/quarter/` - Quarterly weather files
- `data/nc/` - Raw NetCDF data
