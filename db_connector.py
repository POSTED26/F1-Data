import os
import psycopg2
import sqlalchemy 
import logging

"""
Database connector class
    handles the creation of the connection as well as the termination of the connection
"""
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DbConnector():

    def __init__(self):
        """
        Initializes the class with environment variables from an env file
        """

        self.db_name = os.getenv('POSTGRES_DB')
        self.db_user = os.getenv('POSTGRES_USER')
        self.db_pass = os.getenv('POSTGRES_PASSWORD')
        self.db_host = 'localhost'
        self.db_port = os.getenv('CONTAINER_PORT')
        print('init db class')


    def connect(self):
        """
        Sets up a connection to Postgres
        """
        url = sqlalchemy.URL.create('postgresql+psycopg2', self.db_user, self.db_pass, self.db_host, self.db_port, self.db_name)
        
        try:
            self.engine = sqlalchemy.create_engine(url)
            #self.conn = self.engine.connect()
            
            '''
            self.conn = psycopg2.connect(dbname=self.db_name,
                                user=self.db_user,
                                password=self.db_pass,
                                host=self.db_host,
                                port=self.db_port)'''
        except psycopg2.Error as e:
            logger.error(e)

        logger.info('Connection to database established')
        #self.cur = self.conn.cursor()
        
    

    def disconnect(self):
        """
        Closes the connection and cursor to database
        """
        self.conn.close()
        self.cur.close()
        logger.info('Connection to database closed')

