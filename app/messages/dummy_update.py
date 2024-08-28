from library.pg import Pg

pg = Pg()

# setup variable
sql_query = f"""
update messages
set updated_at = current_timestamp
where conversation_id = 35050
"""

if __name__ == '__main__':
   
   pg.execute_sql(sql_query)