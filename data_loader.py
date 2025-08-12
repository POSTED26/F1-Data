import psycopg2
import logging
import pandas as pd
from sqlalchemy import engine, text

import db_connector as dbConn

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

"""
Data loader class:
    handles loading data frames into postgres
    handles creating the tables from sql files

"""

class DbLoader():

    def __init__(self):
        pass
        
    def create_table(self, sql, db):
        with open(sql, 'r') as f:
            sql_file = f.read()
            logger.info(f'Reading SQL file: {sql}')

        with db.engine.connect() as conn:
            conn.execute(text(sql_file))
            logger.info('Executed SQL file')
            conn.commit()
            logger.info('Commited changes to db')
        

        


    def load_table(self, df: pd.DataFrame, table_name, db):
        with db.engine.connect() as conn:
            table_df = pd.read_sql_table(table_name, conn)
            concat_df = pd.concat([table_df, df]).drop_duplicates(keep=False)
            concat_df.to_sql(table_name, conn, if_exists='append', index=False)
            logger.info(f'Loaded data into table {table_name}')