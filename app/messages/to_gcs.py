from library.gcs import Gcs
from library.pg import Pg

# init gcs and postgre instance
gcs = Gcs()
pg = Pg()

# setup variable
gcs_bucket = 'candy-test-bucket'
table_name = 'messages'
sql_query = f"""
select *, date(created_at) as day_partition
from {table_name} 
where date(created_at) = 'dstart'
"""

if __name__ == '__main__':

    # Get day_partition list for data that needs to be updated
    date_list = pg.get_date_list(table_name)

    # Upload date_list.txt to GCS for bigquery job later
    gcs.upload_date_list(gcs_bucket, date_list, table_name)

    # for all day_partition run the query in target table
    data_files = pg.get_data_files(sql_query, table_name, date_list)

    # upload json to gcs
    for data in data_files:
        file_name = data.split('/')[-1]
        day_partition = file_name.split('_')[-1].replace('.json','')
        destination_blob = f"daily/{table_name}/{file_name}"
        gcs.upload_to_gcs(gcs_bucket, data, destination_blob)
    
    # after the task is done, delete temp files
    gcs.delete_temp_directory(table_name)