from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import sys
from .settings import SERVICE_ACCOUNT_PATH

class Bigquery:
    def __init__(self) -> None:
        
        self.client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
        
        # set BQ config
        self.bq_job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            ignore_unknown_values=True,
        )

    def gcs_to_bq(self, gcs_bucket, source_table_name, bq_table_name, date_list):
    
        print(f"Destination table: {bq_table_name}")
        for date in date_list:
            partition_date = date.replace('-','')
            uri = f"gs://{gcs_bucket}/daily/{source_table_name}/{source_table_name}_{date}.json"
            print(f"Data to upload: {uri}")

            table_id_partitioned = bq_table_name + "$" + partition_date
            destination_table_before = self.client.get_table(table_id_partitioned)
            num_rows_before = destination_table_before.num_rows

            # Load data to BQ
            load_job = self.client.load_table_from_uri(
                uri,
                table_id_partitioned,
                location="us-central1",  # Must match the destination dataset location.
                job_config=self.bq_job_config,
            )
            try:
                load_job.result() # Waits for the job to complete.
            except NotFound:
                print(f"No data file found for date {date} ,skipping this date.")
            except:
                for e in load_job.errors:
                    print('ERROR: {}'.format(e['message']))
                sys.exit(1)

            destination_table_after = self.client.get_table(table_id_partitioned)
            num_rows_after = destination_table_after.num_rows
            row_append_count = num_rows_after-num_rows_before
            print(f"Loaded {row_append_count} rows.")

        