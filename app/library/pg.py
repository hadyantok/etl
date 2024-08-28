import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd
import json
from .settings import CANDY_DATABASE_URL

class Pg:
    def __init__(self) -> None:
        
        self.engine = create_engine(CANDY_DATABASE_URL)

    def df_to_pg(self, df, table_name, if_exists='replace'):

        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)

    def execute_sql(self, sql_query):
        with self.engine.connect() as connection:
            # Start a transaction
            trans = connection.begin()
            try:
                # Execute the SQL query
                result = connection.execute(text(sql_query))
                # Commit the transaction
                trans.commit()
                # Return the number of rows affected
                print(f"{result.rowcount} rows affected.")
            except Exception as e:
                # Rollback in case of an error
                trans.rollback()
                print(f"An error occurred: {e}")
        
    def get_date_list(self, table_name):

        # get day_partition that was updated in last 2 days
        date_query = text(f"""
        select distinct TO_CHAR(created_at,'YYYY-MM-DD') as day_partition
        from {table_name}
        where (updated_at >= current_date - interval '2 days' and updated_at < current_date + INTERVAL '1 days')
        order by 1 desc
        """)
        print("Executing Date Query in Target DB")
        with self.engine.connect() as connection:
            result = connection.execute(date_query)
            date_list_tuple = result.fetchall()

        # Extract dates from the result set
        date_list = [date[0] for date in date_list_tuple]

        print(f"Date to run: {date_list}")
        
        return date_list
    
    def dump_query_result_as_file(self, table_name, date, query_results):
        query_results_file_name = table_name + "_" + date + ".json"
        file_path = f"../temp/{table_name}/{query_results_file_name}"
        file = open(file_path, "w")

        query_results_json = [json.dumps(record) for record in query_results]
        for line in query_results_json:
            file.write(line+'\n')
        
        file.close()
        print(f"Saved data for date {date} in {query_results_file_name}")
        return file_path
        
    def get_data_files(self, sql_query, table_name, date_list):

        query_json_wrapper = f"select row_to_json(r) from ({sql_query}) r"
        with self.engine.connect() as connection:
            data_files = []

            # run query loop for each date in date_list
            for date in date_list:
                query_builder = text(query_json_wrapper.replace("dstart",date))
                result = connection.execute(query_builder)
                query_results_tuple = result.fetchall()

                # fix json from list(tuple(dict)) to list(dict) to dict
                query_results = []
                for j in range(0, len(query_results_tuple)):
                    query_results.append(query_results_tuple[j][0])

                result_file = self.dump_query_result_as_file(table_name, date, query_results)
                data_files.append(result_file)

        return data_files


