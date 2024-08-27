# One time script that takes csv from gcs then load it as new table in postgre
# Purpose: to mimic production database situation

from library.gcs import Gcs
from library.pg import Pg

# init gcs and postgre instance
gcs = Gcs()
pg = Pg()

# setup variable
gcs_bucket = 'candy-test-bucket'
blob_path = 'raw/'
csvs = ['conversations.csv', 'messages.csv']

# main
if __name__ == '__main__':
    for csv in csvs:
        gcs_blob = blob_path + csv
        df = gcs.csv_to_df(gcs_bucket, gcs_blob)

        table_name = csv.split('.')[0]
        # upload to postgre
        pg.df_to_pg(df, table_name)


