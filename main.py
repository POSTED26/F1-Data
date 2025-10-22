#import pandas as pd
from pyspark.sql import SparkSession
from urllib.request import urlopen
import requests
import json
import boto3
from botocore.exceptions import NoCredentialsError
import re

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
    
def load_file_to_s3(bucket, file_name):
    """
        Use boto to send file to S3
    """

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY,
                      region_name=AWS_REGEION)
    bucket, key = parse_s3_uri(bucket)
    key = key + file_name
    try:
        s3.upload_file(file_name, bucket, key)
        print("Upload Successful")
        
    except NoCredentialsError:
        print("Credentials not available")
        
    
def parse_s3_uri(s3_uri: str):
    """
    Parse an S3 URI and return the bucket name and key.

    Args:
        s3_uri (str): S3 URI like 's3://my-bucket/path/to/object.json'

    Returns:
        tuple: (bucket_name, key)

    Raises:
        ValueError: If the URI is not a valid S3 path
    """
    

    pattern = r'^s3a://([^/]+)/(.+)$'
    match = re.match(pattern, s3_uri)
    if not match:
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    
    return match.group(1), match.group(2)



def api_to_bronze_s3():
    """
        Use boto to store raw data in AWS S3 (bronze layer)
    """

    endpoints = ['meetings', 'sessions', 'drivers', 'positions']
    calls = [data_extracter.get_race_list(),
                data_extracter.get_sessions_year_list('Race'),
                data_extracter.get_drivers(),
                data_extracter.get_session_result()]


    for i in range(len(endpoints)):
        df = calls[i]
        file_path = f"{endpoints[i]}.json"
        df.to_json(file_path, orient='records', lines=True)
        bucket = S3_RAW_BRONZE_PATH + f'{endpoints[i]}/'
        load_file_to_s3(bucket, file_path)
    
    #meeting_df = data_extracter.get_race_list()
    #meeting_json = meeting_df.to_json('meetings.json', orient='records', lines=True)
    #response = requests.get('https://api.openf1.org/v1/meetings')
    #response.raise_for_status()
    #data = response.json()

    #file_path = "meetings.json"
    #with open(file_path, 'w') as json_file:
    #    json.dump(meeting_df.to_json(), json_file, indent=4)
    
    #bucket = S3_RAW_BRONZE_PATH + 'meetings/'
    #load_file_to_s3(bucket, file_path)
    
    #print(meeting_df.head())



    #meeting_df.coalesce(1).write.mode('overwrite').json(S3_RAW_BRONZE_PATH + 'meetings/')


if __name__ == "__main__":
    #main()
    api_to_bronze_s3()


