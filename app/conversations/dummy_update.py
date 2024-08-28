from library.pg import Pg

pg = Pg()

# setup variable
sql_query = f"""
update conversations
set updated_at = current_timestamp
where user_id = 6051
"""

if __name__ == '__main__':
   
   pg.execute_sql(sql_query)