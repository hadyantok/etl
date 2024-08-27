import pandas as pd
from google.cloud import storage
from .settings import SERVICE_ACCOUNT_PATH

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