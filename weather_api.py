import urllib.request
import time
import shutil
from datetime import datetime, timedelta
import xarray as xr
import pandas as pd
import numpy as np
import zipfile

from multiprocessing import Process, Queue
import os, sys, glob ,re
import struct
import pytz
import pickle

import logging
import logging.config
import logging.handlers
from tqdm import tqdm

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

# use cronitor to monitor the job
import cronitor
# william's api key 
cronitor.api_key = "6a728d6edcea4ec885f014fa1aa76dd9"





import warnings

# Suppress the cfgrib FutureWarning about timedelta decoding
warnings.filterwarnings("ignore", 
                       message="In a future version, xarray will not decode timedelta values*",
                       category=FutureWarning,
                       module="cfgrib.xarray_plugin")






# * Set the timezone to CDT https://www.geeksforgeeks.org/python-time-tzset-function/
if os.name != "nt":
    os.environ["TZ"] = "America/Chicago"
    time.tzset()

# * Set the path to the data folder


sys.path.append(os.path.dirname(__file__))
os.makedirs(os.path.join(os.path.dirname(__file__), "Data"), exist_ok=True)
Data = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data")  # * Path to the data folder

# Add parent directory to the module search path(only need for debugging)
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, parent_dir)

from helper import helper
# print(sys.path)
# print(os.getcwd())

# set the logging configuration for the script
DEBUG = False
log_file = f"{Data}/download.log"
logging.basicConfig(filename=log_file, level=logging.DEBUG if DEBUG else logging.INFO, format="%(asctime)s %(" "levelname)s %(" "message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("weather_api")
logger.debug(f"set os path to {sys.path}")


if not os.path.exists(f"{Data}/meta.csv"):
    meta = pd.DataFrame(columns=["Date", "Time", "RAW", "CSV", "PWW", "Drive"])
    meta.to_csv(f"{Data}/meta.csv", index=False)


def aggregate(df):
    def to_str(x, lens):  # * Function to convert float to string with fixed length
        x_str = "{:.2f}".format(x)
        if (x // 100) == 0:
            return x_str.zfill(lens)
        elif ((-x) // 100) == 0:
            return x_str.zfill(lens + 1)
        else:
            return x_str

    if df.isnull().sum().sum() > 0:
        logger.info(f"Missing values in the data")
        logger.info(df.isnull().sum())

    df.fillna(-999, inplace=True)  # * Fill missing values with -999
    df["valid_time"] = pd.to_datetime(df["valid_time"], format="%Y-%m-%d %H:%M:%S")
    df["longitude"] = df["longitude"] - 360
    df["WhoAmI"] = "+" + df["latitude"].apply(to_str, args=(5,)) + df["longitude"].apply(to_str, args=(6,)) + "/"
    df["sped"] = np.sqrt(df["u10"] ** 2 + df["v10"] ** 2)  # wind speed magnitude
    df["sped100"] = np.sqrt(df["u100"] ** 2 + df["v100"] ** 2)
    df["drct"] = np.arctan2(df["u10"], df["v10"])  # wind direction
    # unit conversion
    df["t2m"] = np.round((df["t2m"] - 273.15) * 9 / 5 + 32)  # convert to degF
    df["d2m"] = np.round((df["d2m"] - 273.15) * 9 / 5 + 32)  # convert to degF
    df["sped"] = np.round(df["sped"] * 2.23694)  # convert from mps to mph
    # df['tcc'] = np.round(df['tcc'] * 100)  # convert to %
    df["drct"] = np.round(df["drct"] * 180 / np.pi + 180)  # convert to deg
    df["DirectHorizontalIrradianceWM2"] = 255 * 5
    df["GlobalHorizontalIrradianceWM2"] = 255 * 5
    df["WindSpeed100mph"] = df["sped100"] * 2.23694  # convert from mps to mph
    df.drop(columns=["longitude", "latitude", "u100", "sped100", "v100", "u10", "v10"], inplace=True)
    ### rename columns base on PW header
    df.rename(
        columns={
            "valid_time": "UTCISO8601",
            "WhoAmI": "WhoAmI",
            "t2m": "tempF",
            "d2m": "DewPointF",
            "sped": "WindSpeedmph",
            "sped100": "WindSpeed100mph",
            "drct": "WindDirection",
            "tcc": "CloudCoverPerc",
        },
        inplace=True,
    )
    ### order columns
    df_new_columnlist = [
        "UTCISO8601",
        "WhoAmI",
        "DewPointF",
        "tempF",
        "GlobalHorizontalIrradianceWM2",
        "CloudCoverPerc",
        "DirectHorizontalIrradianceWM2",
        "WindSpeedmph",
        "WindDirection",
        "WindSpeed100mph",
    ]
    df = df.reindex(columns=df_new_columnlist)
    df = df.sort_values(by=["UTCISO8601"])
    df["UTCISO8601"] = (df["UTCISO8601"].astype("int64") + 2209075200 * 10**9) / (10**9 * 86400)  # convert to days since 1970-01-01
    try:
        df = df.astype(
            {
                "WhoAmI": "str",
                "tempF": "int16",
                "DewPointF": "int16",
                "WindSpeedmph": "int8",
                "WindDirection": "int16",
                "dswrf": "int8",
                "CloudCoverPerc": "int16",
                "WindSpeed100mph": "int16",
            }
        )
    except:
        df = df.astype(
            {
                "WhoAmI": "str",
                "tempF": "int16",
                "DewPointF": "int16",
                "WindSpeedmph": "int8",
                "WindDirection": "int16",
                "GlobalHorizontalIrradianceWM2": "int16",
                "DirectHorizontalIrradianceWM2": "int16",
                "CloudCoverPerc": "int16",
                "WindSpeed100mph": "int16",
            }
        )
    ### change the offset to reduce the size of the data
    df["tempF102"] = df["tempF"] + 115
    df["DewPointF104"] = df["DewPointF"] + 115
    df["WindDirection107"] = df["WindDirection"] / 5
    df["GlobalHorizontalIrradianceWM2_120"] = df["GlobalHorizontalIrradianceWM2"] / 5
    df["DirectHorizontalIrradianceWM2_121"] = df["DirectHorizontalIrradianceWM2"] / 5
    df["WindDirection107"] = df["WindDirection107"].astype(int)
    df["GlobalHorizontalIrradianceWM2_120"] = df["GlobalHorizontalIrradianceWM2_120"].astype(int)
    df["DirectHorizontalIrradianceWM2_121"] = df["DirectHorizontalIrradianceWM2_121"].astype(int)
    return df


def read(path, date, t, shared_queue):
    logger.debug(f"Reading {path}")
    dfs = []
    # * Copy and paste the folder path where you downloaded the data
    keys = ["t2m", "d2m", "u10", "v10", "u100", "v100", "tcc"]
    level = [2, 2, 10, 10, 100, 100, 0]
    for k, l in zip(keys, level):
        if k == "tcc":
            dataset = xr.open_dataset(
                path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"stepType": "instant", "typeOfLevel": "atmosphere"}, "indexpath": ""}
            )  # * Reading Cloud coverage in entire atmosphere level as instant data
        else:
            dataset = xr.open_dataset(
                path, engine="cfgrib", backend_kwargs={"filter_by_keys": {"typeOfLevel": "heightAboveGround", "level": l}, "indexpath": ""}
            )  # add indexpath='' to avoid garbage file
        df = dataset[k].to_dataframe()
        # df.to_csv(f"{path}_{k}.csv")
        dfs.append(df[k])
        dataset.close()
    dfs.append(df["valid_time"])
    df = pd.concat(dfs, axis=1)
    df.reset_index(inplace=True)
    # df.to_csv(fr"Data\csv\{date}\{os.path.basename(path)}.csv")
    df.to_parquet(rf"{Data}/csv/{date}/{date}_{t}/{os.path.basename(path)}.parquet")
    shared_queue.put(df)


def df_to_pww(df, date, t):
    df = aggregate(df)
    logger.info(f"Writing {date}.pww")
    aPWWFileName = rf"{Data}/pww/Forecast_NorthAmerica_Run{date}T{t}Z.pww"
    aPWWVersion = 1
    df_station = pd.read_parquet("station.parquet")
    df.sort_values(by=["UTCISO8601","WhoAmI"], inplace=True) #! sort the data by date and location to match the station data 
    # ref: https://stackoverflow.com/questions/17141558/how-to-sort-a-pandas-dataframe-by-two-or-more-columns
    # convert to ascii null terminated bytes
    num_unique_date = df["UTCISO8601"].nunique()
    unique_dates = df["UTCISO8601"].unique()
    aStartDateTimeUTC = df["UTCISO8601"].min()
    aEndDateTimeUTC = df["UTCISO8601"].max()
    # area=[58, -130, 24, -60] North 58°, West -130°, South 24°, East -60°
    aMinLat = int(df_station["Latitude"].min())
    aMaxLat = int(df_station["Latitude"].max())
    aMinLon = int(df_station["Longitude"].min())
    aMaxLon = int(df_station["Longitude"].max())
    LOC = df_station["WhoAmI"].nunique()

    sta = open("NOAA_station.pkl", "rb").read()
    LOC_FC = 0  # for extra loc variables from table 1
    VARCOUNT = 8  # Set this to the number of weather variable types you have

    # fromate:https://electricgrids.engr.tamu.edu/weather-data/
    # def df_to_PWW(df):
    with open(aPWWFileName, "wb") as file:
        file.write(struct.pack("<h", 2001))
        file.write(struct.pack("<h", 8065))
        file.write(struct.pack("<h", aPWWVersion))
        file.write(struct.pack("<d", aStartDateTimeUTC))
        file.write(struct.pack("<d", aEndDateTimeUTC))
        file.write(struct.pack("<d", aMinLat))
        file.write(struct.pack("<d", aMaxLat))
        file.write(struct.pack("<d", aMinLon))
        file.write(struct.pack("<d", aMaxLon))
        file.write(struct.pack("<h", 0))
        file.write(struct.pack("<i", num_unique_date))  # countNumber of datetime values (COUNT)
        file.write(struct.pack("<i", 0))
        file.write(struct.pack("<i", LOC))  # Number of weather measurement locations (LOC)
        file.write(struct.pack("<h", LOC_FC))  # Loc_FC # Pack the data into INT16 format and write to stream
        file.write(struct.pack("<h", VARCOUNT))

        # Temp in F
        file.write(struct.pack("<h", 102))
        # Dew point in F
        file.write(struct.pack("<h", 104))
        # Wind speed at surface (10m) in mph
        file.write(struct.pack("<h", 106))
        # Wind direction at surface (10m) in 5-degree increments
        file.write(struct.pack("<h", 107))
        # Total cloud cover percentage
        file.write(struct.pack("<h", 119))
        # Wind speed at 100m in mph
        file.write(struct.pack("<h", 110))
        #? Global Horizontal Irradiance in W/m^2 divided by 4 
        file.write(struct.pack("<h", 120))
        #? Direct Horizontal Irradiance in W/m^2 divided by 4
        file.write(struct.pack("<h", 121))
        file.write(struct.pack("<h", 8))  # BYTECOUNT
        #* Write the dates
        for date in unique_dates:file.write(struct.pack("<d", date))
        #* Write the station data
        file.write(sta)
        #* Write the data
        for date in unique_dates:
            # Filter rows by unique date
            rows = df[df["UTCISO8601"] == date]

            for temp in rows["tempF102"]:
                # file.write(temp.encode('utf-8'))
                # file.write(struct.pack('<i', temp))
                file.write(temp.to_bytes(1, "little"))

            for dew_point in rows["DewPointF104"]:
                # file.write(struct.pack('<b', dew_point.to_bytes(1, 'little')))
                file.write(dew_point.to_bytes(1, "little"))
            for wind_speed in rows["WindSpeedmph"]:
                # file.write(struct.pack('<b', wind_speed.to_bytes(1, 'little')))
                file.write(wind_speed.to_bytes(1, "little"))

            for wind_direction in rows["WindDirection107"]:
                # file.write(struct.pack('d', wind_direction))
                file.write(wind_direction.to_bytes(1, "little"))

            for CloudCoverPerc in rows["CloudCoverPerc"]:
                # file.write(struct.pack('<b', CloudCoverPerc.to_bytes(1, 'little')))
                file.write(CloudCoverPerc.to_bytes(1, "little"))

            for WindSpeed100mph in rows["WindSpeed100mph"]:
                # file.write(struct.pack('<b', WindSpeed100mph.to_bytes(1, 'little')))
                file.write(WindSpeed100mph.to_bytes(1, "little"))

            for GlobalHorizontalIrradianceWM2_120 in rows["GlobalHorizontalIrradianceWM2_120"]:
                # file.write(struct.pack('d', GlobalHorizontalIrradianceWM2_120))
                file.write(GlobalHorizontalIrradianceWM2_120.to_bytes(1, "little"))

            for DirectHorizontalIrradianceWM2_121 in rows["DirectHorizontalIrradianceWM2_121"]: #! this will overflow
                # file.write(struct.pack('d', DirectHorizontalIrradianceWM2_121))
                file.write(DirectHorizontalIrradianceWM2_121.to_bytes(1, "little"))
    logger.info(f"Finished writing {date}_{t}.pww")
    return aPWWFileName


def download(date, t):
    attempt = 0
    # * Downloading the data from the NCEP server https://nomads.ncep.noaa.gov/gribfilter.php?ds=gfs_0p25_1hr
    x = 0
    files = []  # store the files that are downloaded
    
    # Set this once at the top - only thing you need to change!
    MAX_X = 384  # Change to 384 for production
    
    # Automatically calculate progress bar total
    max_files = MAX_X + 1  # Always correct!
    progress_bar = tqdm(total=max_files, desc="Downloading weather data", unit="file")
    
    while x <= MAX_X:  # Uses the same variable
        
        try:
            now = time.time_ns()
            #  Modifying the download URL
            url = (
                "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25_1hr.pl?dir=%2Fgfs."
                + date
                + "%2F"
                + t
                + "%2Fatmos&file=gfs.t"
                + t
                + "z.pgrb2.0p25.f"
                + "{:03d}".format(x)
                + "&var_DPT=on&var_TCDC=on&var_TMP=on&var_UGRD=on&var_VGRD=on&lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_100_m_above_ground=on&lev_1000_mb=on&lev_975_mb=on&lev_950_mb=on&lev_925_mb=on&lev_900_mb=on&lev_850_mb=on&lev_800_mb=on&lev_750_mb=on&lev_700_mb=on&lev_650_mb=on&lev_600_mb=on&lev_550_mb=on&lev_500_mb=on&lev_450_mb=on&lev_400_mb=on&lev_350_mb=on&lev_300_mb=on&lev_250_mb=on&lev_200_mb=on&lev_150_mb=on&lev_100_mb=on&lev_70_mb=on&lev_50_mb=on&lev_boundary_layer_cloud_layer=on&lev_entire_atmosphere=on&lev_convective_cloud_layer=on&lev_high_cloud_layer=on&lev_low_cloud_layer=on&lev_middle_cloud_layer=on&subregion=&toplat=58&leftlon=230&rightlon=300&bottomlat=24"
            )
            with urllib.request.urlopen(url) as response, open(f"{Data}/raw/{date}/{date}_{t}/{date}{t}_{x:03d}", "wb") as out_file:
                shutil.copyfileobj(response, out_file)
            files.append(f"{date}{t}_{x:03d}")
            
            # Update progress bar
            progress_bar.update(1)
            progress_bar.set_postfix({"Current file": f"{date}{t}_{x:03d}"})
            
        except Exception as e:
            logger.error(f"Error downloading {date}{t}_{x:03d} {e}")
            time.sleep(60 * 5)  # wait for 5 minutes before trying again
            attempt += 1
            if attempt > 15:
                logger.error(f"Failed to download {date}{t}_{x:03d} after 5 attempts")
                break
            continue
        x += 1 if x < 120 else 3  # hourly data until 5 day, after every 3 hours.
        timer = time.time_ns() - now  # * Calculate the time taken to download the file

        sleep = (2e9 - timer) / 1e9  # just hitting the download limit( NCEP has predetermined download limit 50 hits/minutes)
        if sleep > 0:
            time.sleep(sleep)  # * Delay for exactly 1 second
        logger.debug(f"took {timer//1e6} ms to download file {date}{t}_{x:03d}")
    
    # Close progress bar
    progress_bar.close()
    print(f"✅ Downloaded {len(files)} files successfully!")
    return files
    # logger(f"took {timer} ns to download file {date}{t}_{x:03d}")



    

# Updated main function with progress bar for file processing:
@cronitor.job("zRlIAx")
def main():
    # *-----------------------set up the logger, monitor, and google drive-----------------------*#
    
    # NEW CODE:
    gauth = GoogleAuth(settings_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), "settings.yaml"))
    gauth.ServiceAuth()  # This is the key line for service account authentication
    drive = GoogleDrive(gauth)

    log_file = f"{Data}/download.log"
    logging.basicConfig(filename=log_file, level=logging.DEBUG if DEBUG else logging.INFO, format="%(asctime)s %(" "levelname)s %(" "message)s", datefmt="%Y%m%d-%H:%M:%S")
    logger = logging.getLogger("weather_api")

    # initialize the helper module
    hp = helper(logger)
    
    # *-----------------------create the directories and find the date and time to download-----------------------*#
    check = lambda path: True if os.path.exists(path) else os.makedirs(path)  # create directory if not exists
    today = (datetime.now() - timedelta(days=0)) # get yesterday's date -timedelta(days=1)
    date= today.strftime("%Y%m%d") 
    pww_date = today.strftime("%Y-%m-%d")  # get yesterday's date -timedelta(days=1)
    times = ["00", "06", "12", "18"]
    t = times[datetime.now().hour // 6]  # get the nearest 6 hour interval
    check(rf"{Data}/raw/{date}/{date}_{t}")  # create directory if not exists for those small files
    check(rf"{Data}/csv/{date}/{date}_{t}")
    check(rf"{Data}/pww")
    check(rf"{Data}/csv/compressed")

    # *-----------------------download the data, read, and upload to the cloud-----------------------*#
    logger.info(f"Downloading data for {date}_{t}")
    print(f"🌤️  Starting weather data download for {date}_{t}")
    
    download_files = download(date, t)  # download the data
    
    print("📊 Processing downloaded files...")
    procs = []
    queue = Queue()
    raw_files = glob.glob(rf"{Data}/raw/{date}/{date}_{t}/*")
    
    # Progress bar for file processing
    processing_bar = tqdm(total=len(raw_files), desc="Processing weather files", unit="file")
    
    for file in raw_files:
        p = Process(target=read, args=(file, date, t, queue))
        procs.append(p)
        p.start()
    
    dfs = []
    for p in procs:
        df = queue.get()
        dfs.append(df)
        processing_bar.update(1)
        
    processing_bar.close()
    
    logger.info(f"concatenating {len(dfs)} dataframes for {date}_{t}")
    print(f"🔗 Concatenating {len(dfs)} dataframes...")
    total = len(dfs)
    df = pd.concat(dfs,copy=False)
    dfs.clear()
    for p in procs:
        p.join()
    
    print("💾 Creating PWW file...")
    pww_file = df_to_pww(df, pww_date, t)
    
    print("☁️  Uploading to Google Drive...")
    logger.info(f"Uploading {date}_{t}.pww to the cloud")
    hp.upload_to_drive(drive, "1ydqFR29caPIN2QaC46r-2uytedBzToId", f"{Data}/pww/*",archive_folder_id="1TOTtY8i0o8BabyodeAHefm3j5pFrfvpX")
    hp.archive_folder(drive, "1ydqFR29caPIN2QaC46r-2uytedBzToId", "1TOTtY8i0o8BabyodeAHefm3j5pFrfvpX", timedelta(days=7),date_pattern=re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}"),date_format="%Y-%m-%dT%H")
    
    # *-----------------------delete the raw and preprocess files after compress-----------------------*#
    print("🗑️  Cleaning up files...")
    logger.info(f"deleteing raw {date}files ")
    shutil.rmtree(rf"{Data}/raw/{date}")  # delete the raw files

    logger.info(f"Compressing {date}_{t} data")
    csv_files = glob.glob(rf"{Data}/csv/{date}/{date}_{t}/*")
    with zipfile.ZipFile(f"{Data}/csv/compressed/{date}_{t}.zip", "w") as zipf:
        for f in csv_files:
            zipf.write(f, os.path.basename(f))
    logger.info(f"Uploading {date}_{t}.zip to the cloud , and deleting the files")
    shutil.rmtree(rf"{Data}/csv/{date}")
    
    # *-----------------------update the meta file-----------------------*#
    meta = pd.read_csv(f"{Data}/meta.csv")
    df = pd.DataFrame({"Date": date, "Time": t, "RAW": [raw_files], "CSV": [csv_files], "PWW": pww_file, "Drive": 1}, index=[0])
    meta = pd.concat([meta, df])
    meta.to_csv(f"{Data}/meta.csv", index=False)

    print(f"✅ All done! Downloaded {total} files and uploaded to cloud")
    return f"downloaded {date}_{t} data for total of {total} files, and uploaded to the cloud"






if __name__ == "__main__":
    main()
