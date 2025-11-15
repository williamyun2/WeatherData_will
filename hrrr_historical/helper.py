
import os, re
from glob import glob
from datetime import datetime
import zipfile
# from pydrive2.auth import GoogleAuth
# from pydrive2.drive import GoogleDrive


class helper:
    def __init__(self,logger):
        self.logger = logger
        pass

    def upload_to_drive(self,drive, folder_id, path, overwrite=False,archive_folder_id=None):
        """Upload the files to the google drive
        Args:
            drive: GoogleDrive object
            folder_id: google drive id, the id of the folder to upload the files to
            ext: str, the extension of the files to upload
            overwrite: bool, if True overwrite the existing files
            archive_folder_id: google drive id, the id of the archive folder to move the files to
        """
        cloud_files = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()
        cloud_files_dict= {file["title"]: file for file in cloud_files} # get files metadata 

        # If archive folder ID is provided, get files from the archive folder
        if archive_folder_id:
            archive_files = drive.ListFile({"q": f"'{archive_folder_id}' in parents and trashed=false"}).GetList()
            archive_files_dict = {file["title"]: file for file in archive_files}
            print(f"Found {len(archive_files)} files in the archive folder.")
        else:
            archive_files_dict = {}

        # Combine dictionaries from current and archive folders
        cloud_files_dict = {**cloud_files_dict, **archive_files_dict}

        files = glob(path) # get local files

        for f in files:
            name = os.path.basename(f)
            
            if name in cloud_files_dict:
                if not overwrite:
                    self.logger .info(f"{name} already exists in the cloud. Skipping upload.")
                    continue
                else:
                    self.logger .info(f"Overwriting {name} in the cloud.")
                    cloud_files_dict[name].Trash()

            self.logger .info(f"Uploading {name} ...")
            # Create a new file in the specified folder
            file = drive.CreateFile({"title": name, "parents": [{"id": folder_id}]})
            file.SetContentFile(f)
            file.Upload()
            self.logger .info(f"Uploaded {name} successfully.")


    def archive_folder(self,drive,folder_id,archive_folder_id, limit,date_pattern = re.compile(r"(\d{8})"),date_format="%Y%m%d"):
        """ move the files in the folder to the archive folder
        Args:
            drive : GoogleDrive object
            folder_id : str : folder id of the folder to be archived
            archive_folder_id : str : folder id of the archive folder
            limit : timedelta : limit of time period from today to be archived

        """
        cloud_files = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()
        cloud_files_dict = {file["title"]: file for file in cloud_files}
        limit = datetime.today()- limit
        # loop through the files and move the files to the archive folder
        for file_title, file_metadata in cloud_files_dict.items():
            match = date_pattern.search(file_title)
            if match:
                # Extract and parse the date from the file title
                date = datetime.strptime(match.group(0), date_format)
                if date < limit:
                    # Move the file to the archive folder
                    file_metadata["parents"] = [{"id": archive_folder_id}]
                    file_metadata.Upload()
                    print(f"moved file to archive: {file_title}")


    def zip_file(self, file_path, zip_path,remove=True):
        """Zip a file and remove the original file.
        Args:
            file_path: str, path to the file to be zipped
            zip_path: str, path to the zip file
        """
        with zipfile.ZipFile(zip_path, 'w',compression=zipfile.ZIP_DEFLATED,compresslevel=9) as zipf:
            zipf.write(file_path, os.path.basename(file_path))

        if remove:
            os.remove(file_path) # remove the original file
        
        self.logger .info(f"Zipped {file_path} to {zip_path} and removed the original file.")

if __name__ == "__main__":
    pass