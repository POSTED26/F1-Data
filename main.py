
from urllib.request import urlopen
import json
import boto3
from botocore.exceptions import NoCredentialsError
import re

from dotenv import load_dotenv
import os

import data_extracter



'''
    TODO: Create Logging for ETL process
    TODO: Create Error Handling for ETL process
    TODO: Write better documentation for functions
    TODO: write tests for functions
'''

load_dotenv()

S3_RAW_BRONZE_PATH = os.getenv('S3_RAW_BRONZE_PATH')
S3_STRUCT_BRONZE_PATH = os.getenv('S3_STRUCT_BRONZE_PATH')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')   
AWS_REGEION = os.getenv('AWS_REGION') 


    
def load_file_to_s3(bucket, file_name):
    """
        Use boto to send file to S3
    """

    s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY,
                      aws_secret_access_key=AWS_SECRET_KEY,
                      region_name=AWS_REGEION)
    bucket, key = parse_s3_uri(bucket)
    key = key + file_name
    print(key)
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

    #TODO: get full json in as raw then filter down columns in transition to silver with other things like handling nulls and deduping
    
    endpoints = ['meetings', 'sessions', 'drivers', 'positions']
    calls = [data_extracter.get_race_list(),
                data_extracter.get_sessions_year_list(),
                data_extracter.get_drivers(),
                data_extracter.get_session_result()]


    for i in range(len(endpoints)):
        df = calls[i]
        file_path = f"Data_Files/{endpoints[i]}.json"
        with open(file_path, 'w') as json_file:
            json.dump(df, json_file, indent=4)
        #df.to_json(file_path, orient='records', lines=True)
        bucket = S3_RAW_BRONZE_PATH + f'{endpoints[i]}/'
        load_file_to_s3(bucket, file_path)
    



if __name__ == "__main__":
    api_to_bronze_s3()


