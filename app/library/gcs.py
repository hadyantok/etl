import pandas as pd
from google.cloud import storage
from .settings import SERVICE_ACCOUNT_PATH
import os
import shutil

class Gcs:
    def __init__(self) -> None:
        
        self.client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)

    def csv_to_df(self, gcs_bucket, gcs_blob):
        
        # Initialize the GCS client
        bucket = self.client.bucket(gcs_bucket)
        blob = bucket.blob(gcs_blob)

        # Parse file name
        filename = gcs_blob.split('/')[-1]
        
        # Determine the local path and file name
        local_csv_path = f'../temp/{filename}'
        blob.download_to_filename(local_csv_path)
        
        # Read the CSV file into a DataFrame
        df = pd.read_csv(local_csv_path)

        return df
    
    def upload_to_gcs(self, gcs_bucket, source_file_name, destination_blob):

        # Uploads a file to Google Cloud Storage.
        bucket = self.client.bucket(gcs_bucket)

        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(source_file_name)
    
    def create_temp_directory(self, table_name):
        source_file_name = f"../temp/{table_name}/date_list.txt"
        directory = os.path.dirname(source_file_name)
        if directory:
            os.makedirs(directory, exist_ok=True)

        return source_file_name

    def upload_date_list(self, gcs_bucket, date_list, table_name):

        # Ensure the directory exists
        source_file_name = self.create_temp_directory(table_name)

        # write date_list into a file
        with open(source_file_name, "w") as date_list_file:
            date_list_file.write(repr(date_list))
        print("Saved data to date_list.txt")

        # upload to gcs 
        destination_blob = f"daily/{table_name}/date_list.txt"
        self.upload_to_gcs(gcs_bucket, source_file_name, destination_blob)
        print(f"date_list.txt uploaded to gs://{gcs_bucket}/{destination_blob} ")

    def download_date_list(self, gcs_bucket, table_name):

        # create temporary directory
        destination_file_name = self.create_temp_directory(table_name)
        
        # download to local
        bucket = self.client.bucket(gcs_bucket)
        blob = bucket.blob(f"daily/{table_name}/date_list.txt")
        blob.download_to_filename(destination_file_name)

        # read file
        with open(destination_file_name, "r") as date_list_file:
            date_list = date_list_file.read().replace("'","").strip('][').split(', ')
        
        print(f"Date to run: {date_list}")
        return date_list

    def delete_temp_directory(self, table_name):
        directory_path = f"../temp/{table_name}/"
        try:
            if os.path.exists(directory_path) and os.path.isdir(directory_path):
                shutil.rmtree(directory_path)
                print(f"Local directory {directory_path} and all its contents have been deleted.")
            else:
                print(f"The local directory {directory_path} does not exist.")
        except Exception as e:
            print(f"An error occurred while deleting the directory: {e}")