#import pandas as pd
from pyspark.sql import SparkSession
from urllib.request import urlopen

from dotenv import load_dotenv
import os

import data_extracter
import data_process
import data_loader
import db_connector


'''
    TODO: Create Logging for ETL process
    TODO: Create Error Handling for ETL process
    TODO: Modify to use Spark where possible
'''

load_dotenv()

S3_RAW_BRONZE_PATH = os.getenv('S3_RAW_BRONZE_PATH')
S3_STRUCT_BRONZE_PATH = os.getenv('S3_STRUCT_BRONZE_PATH')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')   
AWS_REGEION = os.getenv('AWS_REGION') 

def main():

    

    # extract data from api
    meeting_df = data_extracter.get_race_list()
    sessions_df = data_extracter.get_sessions_year_list('Race')
    driver_df = data_extracter.get_drivers()
    pos_df = data_extracter.get_session_result()


    # transform

    driver_df = data_process.driver_process(driver_df)

    driver_cummulative_df = driver_df.merge(sessions_df, how='inner', on=['session_key', 'meeting_key'])
    driver_cummulative_df = driver_cummulative_df.merge(pos_df, how='inner', on=['session_key', 'driver_number'])
    driver_cummulative_df = driver_cummulative_df.merge(meeting_df, how='inner', on=['meeting_key', 'country_name', 'year'])

    
    # load
    db = db_connector.DbConnector()
    loader = data_loader.DbLoader()
    db.connect()
    #create tables
    loader.create_table('meetings.sql', db)
    loader.create_table('sessions.sql', db)
    loader.create_table('drivers.sql', db)
    loader.create_table('session_results.sql', db)
    loader.create_table('driver_rolling_stats.sql', db)
    
    # load the tables
    loader.load_table(meeting_df, 'meetings', db)
    loader.load_table(sessions_df, 'sessions', db)
    loader.load_table(driver_df, 'drivers', db)
    loader.load_table(pos_df, 'session_results', db)
    loader.load_table(driver_cummulative_df, 'driver_rolling_stats', db)
    


def api_to_bronze_s3():
    """
        Use pySpark to store raw data in AWS S3 (bronze layer)
    """
    spark = SparkSession.builder \
        .appName("APItoS3").getOrCreate()
    #   .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    #   .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    #   .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    #   .getOrCreate()
    




    spark._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "60000")  # 60s
    spark._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "60000")
    spark._jsc.hadoopConfiguration().set("fs.s3a.retry.interval", "1000")
    spark._jsc.hadoopConfiguration().set("fs.s3a.retry.limit", "10")
    spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
    spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    meeting_df = data_extracter.get_race_list()

    #print(meeting_df.show())
    #meeting_df.repartition(2)

    meeting_df.coalesce(1).write.mode('overwrite').json(S3_RAW_BRONZE_PATH + 'meetings/')


if __name__ == "__main__":
    #main()
    api_to_bronze_s3()


