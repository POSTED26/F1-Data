import boto3
import json

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))
    
    glue = boto3.client('glue', region_name='us-west-2') 
    job_name = 'meeting_to_parquet'

    try:
        print(f"Starting Glue job: {job_name}")
        response = glue.start_job_run(
            JobName=job_name,
            Arguments={
                "--JOB_NAME": job_name,
            }
        )
        job_run_id = response['JobRunId']
        print(f"Glue job started successfully: {job_run_id}")
        return {
            'statusCode': 200,
            'body': f"Glue job started successfully: {job_run_id}"
        }

    except Exception as e:
        print(f"Error starting Glue job: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error starting Glue job: {str(e)}"
        }
