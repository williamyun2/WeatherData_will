import os, re, time, pathlib ,datetime ,glob ,sys 
import urllib.request
import shutil
import pandas as pd
import numpy as np
import concurrent.futures

import json
from bs4 import BeautifulSoup
from b3d import B3D

import logging
import logging.config
import logging.handlers

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

from tqdm import tqdm

import cronitor

cronitor.api_key = "dc75b70251984b21b2d51290b7215dcf"

# * Set the path to the data folder

sys.path.append(os.path.dirname(__file__))
Data = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data")  # * Path to the data folder
# set the logging configuration for the script

# # * Set the timezone to CDT https://www.geeksforgeeks.org/python-time-tzset-function/
if os.name != "nt":
    os.environ["TZ"] = "America/Chicago"
    time.tzset()


DEBUG = False
log_file = f"{Data}/download.log"
logging.basicConfig(filename=log_file, level=logging.DEBUG if DEBUG else logging.INFO, format="%(asctime)s %(" "levelname)s %(" "message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("time_lapse")
logger.debug(f"set os path to {sys.path}")



def get_date(url):
    """Get the date from the url"""
    try:
        patten = re.compile(r"(\d{8})T(\d{6})-(\d{2})-?")
        match = patten.search(url)
        return [match.groups()[0] + match.groups()[1], match.groups()[2]]
    except:
        return [None, None]


def get_urls(url):
    response = urllib.request.urlopen(url)
    soup = BeautifulSoup(response, "html5lib")
    urls = [i["href"] for i in soup.find_all("a", href=True)]
    urls = [i for i in urls if i.endswith(".json")]
    return urls


def decode(path):
    with open(path, "r") as f:
        data = json.load(f)
        df = pd.json_normalize(data, "features")
    return df


def check():
    check_dir = lambda path: True if os.path.exists(path) else os.makedirs(path)

    def check_meta(path):
        if not os.path.exists(path):
            meta_data = pd.DataFrame(columns=["url", "downloaded", "date", "group"])
            meta_data.set_index("url", inplace=True)
            meta_data.to_csv(path, index=True)

    check_dir("Data")
    check_dir("Data/1D")
    check_dir("Data/3D")
    check_meta("Data/meta_data1d.csv")
    check_meta("Data/meta_data3d.csv")


    
    





def get_data(base_url, meta_path, typ="1D", limit=None):
    meta= pd.read_csv(meta_path)
    check_dir = lambda path: True if os.path.exists(path) else os.makedirs(path)
    df = pd.DataFrame(get_urls(base_url), columns=["url"], dtype=str)
    df["date"], df["group"] = zip(*df["url"].map(get_date))  # split the date and group
    df.dropna(inplace=True)  # drop the data that does not have date
    df["downloaded"] = False  # assume is not downloaded
    df["date"] = pd.to_datetime(df["date"], utc=True)  # set the time zone
    df["group"] = df["group"]
    meta["date"] = pd.to_datetime(meta["date"], utc=True)
    df=df[~df["url"].isin(meta["url"])]  # filter the data that is already downloaded
    
    # Limit for testing
    if limit is not None:
        df = df.head(limit)
    
    logger.info(f"Downloading {len(df)} data files from {base_url}")
    i = 0
    fail = 0
    attempt = 0
    
    total_files = max(len(df) - 2, 0)
    with tqdm(total=total_files, desc=f"Downloading {typ}", unit="file") as pbar:
        try:
            while i+2 < (len(df)): # the last two data is no always available
                url = base_url + os.path.basename(df.iloc[i]["url"])
                logger.debug(f"Downloading {url}")
                path = f"Data/{typ}/{df.iloc[i]['date'].strftime('%Y%m%d')}"
                check_dir(path)
                now = time.time_ns()
                try:  # * first try to download the data
                    response = urllib.request.urlopen(url).read()
                except Exception as e:
                    logger.info(e)
                    attempt += 1
                    if attempt > 2:
                        fail+=1
                        i += 1
                    time.sleep(2)  # wait for 2 seconds before trying again
                    continue
                
                # meta = meta.append(df.iloc[i])
                get_timer = time.time_ns() - now
                try:  # * then try to save the data to csv
                    data = json.loads(response.decode("utf-8"))
                    df_ = pd.json_normalize(data, "features")
                    df_['date'] = df.iloc[i]['date'] # add the date to the data
                    df_.to_parquet(f"{path}/{pathlib.Path(os.path.basename(df.iloc[i]['url'])).stem}.parquet")
                except Exception as e:  # if it fails then save the data as json
                    logger.info(e)
                    with open(f"{path}/{df.iloc[i]['url']}", "w") as out_file:
                        json.dump(data, out_file)
                decode_timer = time.time_ns() - now  # * Calculate the time taken to download the file

                sleep = (2e9 - decode_timer) / 1e9  # just hitting the download limit( NCEP has predetermined download limit 50 hits/minutes)
                if sleep > 0:
                    time.sleep(sleep)  # * Delay for exactly 1 second
                logger.debug(f"took {get_timer//1e6} ms to download file {df.iloc[i]['url']} , {decode_timer//1e6} ms to decode the file")
                df.loc[i, "downloaded"] = True
                i += 1
                pbar.update(1)
        except Exception as e:
            logger.info(f"Data downloaded interrupted casue by{e} ")
        
    
    logger.info(f"Download completed, failed to download {fail} files")
    meta = pd.concat([meta, df], axis=0)
    meta.drop_duplicates(inplace=True, keep="last")  # keep the last update of the data
    meta.to_csv(meta_path, index=False)
    return len(df)








def df_to_b3d(df, path): #? this method is not optimized, may be try geojson format https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html
    
    print(f"Starting df_to_b3d for {path}")
    print(f"Input dataframe shape: {df.shape}")
    
    b3d = B3D()
    df['lat'], df['lon'] = df['geometry.coordinates'].str[0], df['geometry.coordinates'].str[1]
    df.rename(columns={'properties.Ex':'Ex', 'properties.Ey':'Ey', 'properties.distance_nearest_station':'near'}, inplace=True)

    df = df[['lat','lon','Ex','Ey','near','date']].copy()  # Make explicit copy
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    print(f"After cleaning, dataframe shape: {df.shape}")
    
    df = df.sort_values(by=['date', 'lat', 'lon'])
    unique_dates = df['date'].unique()
    unique_lon = df['lon'].unique()
    unique_lat = df['lat'].unique()
    
    print(f"Unique dates: {len(unique_dates)}")
    print(f"Unique lons: {len(unique_lon)}")
    print(f"Unique lats: {len(unique_lat)}")
    
    lon, lat = np.meshgrid(unique_lon, unique_lat)
    lat = lat.flatten()
    lon = lon.flatten()
    
    print(f"Grid size: {len(lat)} points")
    
    b3d.lat= np.array(lat, dtype=np.double)
    b3d.lon =np.array(lon, dtype=np.double)
    ex =np.empty([len(unique_dates), len(lon)])
    ey =np.empty([len(unique_dates), len(lon)])
    sta_flag= True
    n_station = np.ones_like(lon) * -1 # Initialize with -1 to indicate missing data
    df.set_index(['date', 'lon', 'lat'], inplace=True) # reference https://pandas.pydata.org/docs/user_guide/advanced.html
    
    print(f"Starting date processing loop for {len(unique_dates)} dates...")
    for i, date in enumerate(unique_dates):
        # No individual date printing - just process silently
        
        date_data = df.loc[date]  # Extract data for the current date
        for j, (lo, la) in enumerate(zip(lon, lat)):
            try:
                ex[i, j] = date_data.loc[(lo,la)]['Ex']
                ey[i, j] = date_data.loc[(lo,la)]['Ey']
                if sta_flag:
                    n_station[j] = date_data.loc[(lo,la)]['near']
            except KeyError:
                # If the key is not found, ex[i, j] and ey[i, j] remain NaN
                pass
        sta_flag = False
    
    print("Date processing complete, creating B3D object...")
    df.reset_index(inplace=True)
    b3d.ex = np.array(ex, dtype=np.single)
    b3d.ey = np.array(ey, dtype=np.single)
    b3d.n_station = np.array(n_station, dtype=np.double)
    b3d.time_0 = int(unique_dates.astype("int64")[0] /(10**9)+86400) # convert to days since 1970-01-01
    today_begin = unique_dates[0].replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    b3d.time = np.uint32((unique_dates.astype("int64")- unique_dates.astype("int64")[0])/10**9) # convert to seconds since the offset
    b3d.time_units = 1
    logger.info(f" converting {len(unique_dates)} dates to b3d")
    b3d.comment = os.path.basename(path)[0]
    
    print(f"Writing B3D file to {path}")
    b3d.write_b3d_file(path)
    print(f"B3D file written successfully!")










def process_data(typ="1D",day=(datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')):
    print(f"process_data called with typ={typ}, day={day}")
    pattern = f"Data/{typ}/{day}/*.parquet"
    print(f"Looking for files matching: {pattern}")
    files=glob.glob(pattern)
    print(f"Found {len(files)} parquet files: {files}")
    if len(files)!=0:
        logger.info(f"Processing {len(files)} files, for {day}")
        print(f"Processing {len(files)} files, for {day}")
        try:
            df = pd.concat([pd.read_parquet(f) for f in files])
            print(f"Concatenated DataFrame shape: {df.shape}")
            output_path = f"Data/{typ}/{day}_{typ}.b3d"
            print(f"Creating B3D file: {output_path}")
            df_to_b3d(df, output_path)
            print(f"B3D file created successfully!")
        except Exception as e:
            print(f"Error during processing: {e}")
            import traceback
            traceback.print_exc()
    else:
        logger.info(f"No data found for {day}")
        print(f"No data found for {day}")
    return len(files)



def upload_to_drive(drive_service, folder_id, path):
    """Upload the files to the google drive
    Args:
        drive_service: Google Drive API service object
        folder_id: str, the id of the folder to upload the files to
        path: str, the path pattern of the files to upload
    """
    print(f"upload_to_drive called with folder_id={folder_id}, path={path}")
    try:
        # List files in the folder (with Shared Drive support)
        print("Getting list of files already in cloud...")
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        cloud_files = response.get('files', [])
        
        cloud_files = [file["name"] for file in cloud_files]
        print(f"Found {len(cloud_files)} files in cloud: {cloud_files}")
        check = lambda file: file in cloud_files
        
        print(f"Looking for local files matching: {path}")
        files = glob.glob(path)
        print(f"Found {len(files)} local files to upload: {files}")
        
        for f in files:
            name = os.path.basename(f)
            print(f"Checking file: {name}")
            if check(name):
                logger.info(f"{name} already exists in the cloud.")
                print(f"{name} already exists in the cloud.")
                continue
            logger.info(f"Uploading {name}  ...")
            print(f"Uploading {name}  ...")
            
            file_metadata = {
                'name': name,
                'parents': [folder_id]
            }
            
            media = MediaFileUpload(f, resumable=True)
            file = drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
            print(f"Successfully uploaded {name} with ID: {file.get('id')}")
            
    except HttpError as error:
        logger.error(f"An error occurred during upload: {error}")
        print(f"An error occurred during upload: {error}")
    except Exception as e:
        print(f"Unexpected error during upload: {e}")
        import traceback
        traceback.print_exc()

def process_past_files():
    """Process the files that are already downloaded"""
    today = datetime.datetime.now().strftime('%Y%m%d')
    
    for f in ['1D','3D']:
        folders = glob.glob(fr"Data/{f}/*/")
        existing_files = glob.glob(f"Data/{f}/*.b3d")
        for folder in folders:
            flag = True
            day = os.path.basename(os.path.dirname(folder))
            
            # Skip today's folder - it should be processed by main logic
            if day == today:
                print(f"Skipping today's folder: {day}")
                continue
                
            for e in existing_files:
                if day in e:
                    flag=False
            if flag:
                print("Processing{} for {}".format(f,day))
                process_data(f, day)

       

@cronitor.job('fY3GQw')
def main():
    DEBUG = False  # set to true to run the script in debug mode
    check()  # check if the data folder exists
    
    # *-----------------------Calculate and log current time and yesterday's date-----------------------*#
    current_time = datetime.datetime.now()
    yesterday = (current_time - datetime.timedelta(days=1)).strftime('%Y%m%d')
    
    # *-----------------------set up the logger, monitor, and google drive-----------------------*#
    
    # Setup Google Drive API with service account
    SCOPES = ['https://www.googleapis.com/auth/drive']
    SERVICE_ACCOUNT_FILE = 'pydrive2-461721-19b0d14ec905.json'

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    drive_service = build('drive', 'v3', credentials=creds)
    
    log_file = f"Data/download.log"
    logging.basicConfig(filename=log_file, level=logging.DEBUG if DEBUG else logging.INFO, format="%(asctime)s %(" "levelname)s %(" "message)s", datefmt="%Y%m%d-%H:%M:%S")
    logger = logging.getLogger("weather_api")
    
    # Log the timing information
    logger.info(f"Current time: {current_time}")
    logger.info(f"Yesterday calculated as: {yesterday}")
    print(f"Current time: {current_time}")
    print(f"Processing data for date: {yesterday}")
    
    # *-----------------------Download the data from the Efield-----------------------*#
    url_1d = "https://services.swpc.noaa.gov/json/lists/rgeojson/US-Canada-1D/"
    url_3d = "https://services.swpc.noaa.gov/json/lists/rgeojson/InterMagEarthScope/"
    meta1d = "Data/meta_data1d.csv"
    meta3d = "Data/meta_data3d.csv"
    num_1d = get_data(url_1d, meta1d, "1D")  # read the meta data)
    num_3d = get_data(url_3d, meta3d, "3D")  # downloads 4 filessssssssssssssss
    
    print(f"Downloaded {num_1d} 1D files and {num_3d} 3D files")
    
    # *-----------------------process the data for the day and upload to the cloud-----------------------*#
    current_hour = current_time.hour
    print(f"Current hour: {current_hour}")
    
    if (current_hour < 2):  # process the data at midnight
        print("Time condition met - starting processing")
        print(f"Starting data processing for {yesterday}")
        
        # Process yesterday's complete data
        print("Processing 1D data...")
        num_1d_p = process_data("1D", yesterday)
        print(f"1D processing returned: {num_1d_p}")
        
        print("Processing 3D data...")
        num_3d_p = process_data("3D", yesterday)  
        print(f"3D processing returned: {num_3d_p}")
        
        print("Calling process_past_files...")
        process_past_files()  # in case the data was not processed or server was down
        
        print("Data processing completed")
        logger.info("Data processing completed")
        
        print("Starting uploads...")

        # test mode
        upload_to_drive(drive_service, "1fnw5Olj7OOGbip19UTMgEktGUZ7dMapi", "Data/1D/*b3d")
        upload_to_drive(drive_service, "1emw7QyS1ICXBt8OhYBZ1GN82mDl36PpY", "Data/3D/*b3d")    

        # production mode
        # upload_to_drive(drive_service, "1pItMc-ViWiRbY6G49sLlmP_0B5fRMh1W", "Data/1D/*b3d")
        # upload_to_drive(drive_service, "1JIOe_ANudOk2zW9v9LpSJ9UeKX-Zo4Ch", "Data/3D/*b3d")    

        print("Upload calls completed")
        
        return f" completed! downloaded {num_1d} 1D files and {num_3d} 3D files, processed {num_1d_p} 1D files and {num_3d_p} 3D files"
    else:
        print("Time condition not met - skipping processing")
        return f" completed! downloaded {num_1d} 1D files and {num_3d} 3D files"




if __name__ == "__main__":
    #process_data("1D")
    main()
    #process_past_files()
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #         f1=executor.submit(process_data, "1D")
    #         f2=executor.submit(process_data, "3D")
    #         concurrent.futures.wait([f1,f2])
    #         print("Data processing completed")
    #main()