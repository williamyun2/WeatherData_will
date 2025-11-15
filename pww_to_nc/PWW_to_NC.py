# PWW_to_NC.py
import struct
import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
from datetime import datetime, timezone, timedelta
import xarray as xr

name_decoder = {
    101: 'temp_c_2m',
    102: 'temp_F_2m',
    103: 'dewpoint_c_2m',
    104: 'dewpoint_F_2m',
    105: 'wind_speed_10m_mps',
    106: 'wind_speed_10m_mph',
    107: 'wind_direction_10m_deg',
    109: 'wind_speed_100m_mps',
    110: 'wind_speed_100m_mph',
    111: 'wind_speed_80m_to_100m_mps',
    112: 'wind_speed_80m_to_100m_mph',
    119: 'total_cloud_cover_percent',
    120: 'global_horizontal_irradiance_wm2',
    121: 'direct_horizontal_irradiance_wm2',
    122: 'vertically_integrated_smoke_mgm2',
    135: 'wind_gust_surface_mps',
    136: 'wind_gust_surface_mph',
    150: 'percent_frozen_precip_surface',
    151: 'precipitation_rate_surface_mmhr',
    1101: 'temp_c_2m_mult_100',
    1103: 'dewpoint_c_2m_mult_100',
    1105: 'wind_speed_10m_mps_mult_100',
    1109: 'wind_speed_100m_mps_mult_100',
    1120: 'global_horizontal_irradiance_wm2_full',
    1121: 'direct_horizontal_irradiance_wm2_full',
    1122: 'vertically_integrated_smoke_mgm2_full'
}

equation_decoder = {
    101: lambda x: x - 100,
    102: lambda x: x - 115,
    103: lambda x: x - 100,
    104: lambda x: x - 115,
    105: lambda x: x,  # Already in m/s
    106: lambda x: x,  # Already in mph
    107: lambda x: x * 5,  # Convert to degrees
    109: lambda x: x,  # Already in m/s
    110: lambda x: x,  # Already in mph
    111: lambda x: x,  # Conversion logic handled elsewhere
    112: lambda x: x,  # Conversion logic handled elsewhere
    119: lambda x: x,  # Already in percentage
    120: lambda x: x * 5,  # Convert to full W/m^2
    121: lambda x: x * 5,  # Convert to full W/m^2
    122: lambda x: 10 ** (x / 40),  # Convert from log to mg/m^2
    135: lambda x: x,  # Already in m/s
    136: lambda x: x,  # Already in mph
    150: lambda x: x,  # Already in percentage
    151: lambda x: x,  # Already in mm/hour
    1101: lambda x: x / 100,  # Convert to degrees C
    1103: lambda x: x / 100,  # Convert to dewpoint in C
    1105: lambda x: x / 100,  # Convert to wind speed in m/s
    1109: lambda x: x / 100,  # Convert to wind speed in m/s
    1120: lambda x: x,  # Already in full W/m^2
    1121: lambda x: x,  # Already in full W/m^2
    1122: lambda x: x  # Already in full mg/m^2
}

def PWW_to_NC(pww_filename, offset_time=False):
    def read_null_terminated_string(file):
        chars = []
        while True:
            char = file.read(1)
            if char == b"\x00":
                break
            chars.append(char)
        return b"".join(chars).decode("ascii")

    with open(pww_filename, "rb") as file:

        # Read and unpack the file header
        header = struct.unpack("<hhhddddddh", file.read(56))
        start_datetime = datetime.fromtimestamp(header[3] * 86400, timezone.utc) - relativedelta(years=70, day=1)
        end_datetime = datetime.fromtimestamp(header[4] * 86400, timezone.utc) - relativedelta(years=70, day=1)

        print(f"filekey :{header[0]} {header[1]}| version:{header[2]}")
        print(f"Date range:{start_datetime} --{end_datetime}")
        print(f"lat:[{header[5]}:{header[6]}] | lon:[{header[7]}:{header[8]}]")
        if header[9]:  # if metadata is present
            print(f"file_name: {read_null_terminated_string(file)}")
        header = struct.unpack("<iiihh", file.read(16))
        unique_dates = header[0]
        LOC = header[2]
        sample_time = header[1]
        vars = header[4]
        print(f"unique_dates:{unique_dates} |LOC:{LOC} |sameple time {sample_time}s |weather_var:{header[4]}")
        vars_name = [struct.unpack("<h", file.read(2))[0] for _ in range(vars)]
        print(f"vars name:{vars_name}")
        decode_name = [name_decoder[var] for var in vars_name]
        print(f"decode name:{decode_name}")
        vars_varification = struct.unpack("<h", file.read(2))[0]
        if vars_varification != vars:
            print("vars_varification failed")
        # Read unique dates
        time_offset = timedelta(days=0) # if offset_time is True, then add 1 day to the date
        if offset_time:
            time_offset = timedelta(days=1)
        if sample_time == 0:  # if  time is present
            dates = np.array([struct.unpack("<d", file.read(8))[0] for _ in range(unique_dates)]).astype("float64")
            dates = pd.to_datetime(dates * (10**9 * 86400) - 2209161600 * 10**9, unit="ns")
            print(f"read unique dates:{dates[0]}...")
        else:
            print(f"smaple time is  present, create unique dates base on time sample {unique_dates}")
            dates = pd.date_range(start=start_datetime, end=end_datetime - time_offset, periods=unique_dates)
            # print(f"dates:{dates}")
        
        # Remove timezone info from dates for NetCDF compatibility
        dates = dates.tz_localize(None)
        
        stations = []
        for row in range(LOC):
            lat = struct.unpack("<d", file.read(8))[0]
            lon = struct.unpack("<d", file.read(8))[0]
            alt = struct.unpack("<h", file.read(2))[0]
            # whoami = file.read(15).split(b'\x00', 1)[0].decode('ascii')
            whoami = read_null_terminated_string(file)
            country = read_null_terminated_string(file)
            region = read_null_terminated_string(file)
            stations.append((lat, lon, alt, whoami, country, region))
            # print(f"lon:{lon}, lat:{lat}, alt:{alt}, whoami:{whoami}, country:{country}, region:{region}")
        stations_df = pd.DataFrame(stations, columns=["Latitude", "Longitude", "ElevationMeters", "WhoAmI", "Country2", "Region"])
        weather_data = []
        data = np.fromfile(file, dtype=np.uint8)  #! this code didn't consider the variable type other than int8

    # base on the raw data, decode and create nc file
    lons = stations_df.Longitude.to_numpy()
    lats = stations_df.Latitude.to_numpy()
    lon_dim = np.sum(np.abs(np.diff(lons)) > 10) + 1  # the the shape of lon and lat dimension
    # print(f"lon_dim:{lon_dim}")
    data = data.reshape((unique_dates, vars, lon_dim, -1))
    print(f"data shape: times:{unique_dates}, vars:{vars}, lons:{lon_dim}, lats:{data.shape[3]}")
    data = np.where(data == 255, np.nan, data)
    time_offset = timedelta(days=0)

    ds = xr.Dataset(
        data_vars={name_decoder[var]: (["time", "lat", "lon"], equation_decoder[var](data[:, i, :, :])) for i, var in enumerate(vars_name)},
        coords={
            "time": dates,
            "latitude": (["lat", "lon"], lats.reshape(lon_dim, -1)),
            "longitude": (["lat", "lon"], lons.reshape(lon_dim, -1)),
            "region": (["lat", "lon"], stations_df.Region.to_numpy().reshape(lon_dim, -1)),
        },
    )
    ds = ds.astype(np.int16)
    # ds=ds.astype(np.float16)
    return ds




































