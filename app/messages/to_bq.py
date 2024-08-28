from library.gcs import Gcs
from library.bq import Bigquery

# init gcs and Bq instance
gcs = Gcs()
bq = Bigquery()

# setup variable
gcs_bucket = 'candy-test-bucket'
table_name = 'messages'
bq_table_name = 'staging.fd_messages'

if __name__ == '__main__':

    # Get date_list from previous gcs job
    date_list = gcs.download_date_list(gcs_bucket, table_name)

    # upload json in gcs to bq by partition
    bq.gcs_to_bq(gcs_bucket, table_name, bq_table_name, date_list)

    # after the task is done, delete temp files
    gcs.delete_temp_directory(table_name)