import xarray as xr
import pyproj # added 8/29 for GetStation
import netCDF4
import pandas as pd
import multiprocessing
import os
import glob
import re
import json

import yaml
import time
import cdsapi

import requests # added 8/29 for GetStation

import numpy as np
import matplotlib.pyplot as plt
from datetime import date
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
import queue
from tqdm import tqdm
import zipfile # added 8/29 for GetStation
import geopandas as gpd
import rasterio
from rasterio.transform import from_origin
from rasterio.warp import transform

import psutil
import seaborn as sns
from functools import partial
from shapely.geometry import Point
import affine
import logging
from functools import wraps
import dask.dataframe as dd


# Create object of the pyproj.Proj class for the Lambert Azimuthal Equal Area projection
proj = pyproj.Proj(proj='laea',  # Lambert Azimuthal Equal Area
            lon_0=-100.0,  # Longitude_of_Projection_Center
            lat_0=45,   # Latitude_of_Projection_Center
            a=6370997.0  # Semi-major_Axis:
        )

def to_str(x, lens):
    x_str = "{:.2f}".format(x)
    if (x // 100) == 0:
        # lon=format(lon, '.2f')
        return x_str.zfill(lens)
    elif ((-x) // 100) == 0:
        # lon = format(lon, '.2f')
        return x_str.zfill(lens + 1)
    else:
        return x_str

print('Opening the reference file')
# open the reference file that contain the station location lat ,lon and elevation

df = xr.open_dataset(
    r"C:\class\code\service_testing\cds\hawaii_orog.grib",
    engine="cfgrib",
)
print(df)
df = df.to_dataframe()
df.reset_index(inplace=True)

df['station_id'] = df['longitude'].apply(to_str, args=(6,)) + '_' + df['latitude'].apply(to_str, args=(5,))
df['station_id'].drop_duplicates(inplace=True)
print('probs problem here')
df['Country2']=''
df['Region'] = ''
df['WMO'] = 0
df['ElevationMeters'] = df['z'] / 9.80665
df['ICAO'] = ''
print('Anything here?')
df = df[['station_id', 'latitude', 'longitude', 'ElevationMeters', 'ICAO', 'WMO', 'Region', 'Country2']]
df.rename(columns={"station_id": "Name", 'latitude': 'Latitude', 'longitude': 'Longitude'}, inplace=True)
df.set_index('Name', inplace=True)
df.to_parquet('hawaii_station.parquet', index=False)