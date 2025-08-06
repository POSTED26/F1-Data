import psycopg2
import logging
import pandas as pd

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

        db.cur.execute(sql_file)
        logger.info('Executed SQL file')
        db.conn.commit()
        logger.info('Commited changes to db')


    def load_table(self, df: pd.DataFrame, table_name, db):
        df.to_sql(table_name, db.conn, if_exists='replace', index='False')
        logger.info(f'Loaded data into table {table_name}')