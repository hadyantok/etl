import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from .settings import CANDY_DATABASE_URL

class Pg:
    def __init__(self) -> None:
        
        self.engine = create_engine(CANDY_DATABASE_URL)

    def df_to_pg(self, df, table_name, if_exists='replace'):

        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)

    def execute_sql(self, sql_query):
        # Establish a connection to the database
        with self.engine.connect() as connection:
            # Execute the SQL query
            result = connection.execute(sql_query)
            # Return the number of rows affected
            return result.rowcount