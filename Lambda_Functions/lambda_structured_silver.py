import boto3
import json

def lambda_handler(event, context):
    print("Event received:", json.dumps(event))
    
    glue = boto3.client('glue', region_name='us-west-2') 
    job_names = ['sessions_silver', 'meetings_silver', 'dirvers_silver', 'positions_silver']
    #job_names = ['drivers_silver']
    
    results = []
    errors = []
    
    for job_name in job_names:
        try:
            print(f"Starting Glue job: {job_name}")
            response = glue.start_job_run(
                JobName=job_name,
                Arguments={
                    "--JOB_NAME": job_name,
                }
            )
            job_run_id = response['JobRunId']
            print(f"Glue job {job_name} started successfully: {job_run_id}")
            results.append({
                'job_name': job_name,
                'job_run_id': job_run_id,
                'status': 'started'
            })
            
        except Exception as e:
            error_msg = f"Error starting {job_name}: {str(e)}"
            print(error_msg)
            errors.append({
                'job_name': job_name,
                'error': str(e)
            })
    
    # Return after all jobs are processed
    if errors:
        return {
            'statusCode': 207,  # Multi-status
            'body': json.dumps({
                'message': 'Some jobs failed to start',
                'successful': results,
                'failed': errors
            })
        }
    else:
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'All Glue jobs started successfully',
                'jobs': results
            })
        }