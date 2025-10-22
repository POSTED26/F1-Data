import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3


def empty_s3_path(bucket, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=prefix).delete()




## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

endpoints = ['meetings', 'drivers', 'sessions', 'positions']

for i in range(len(endpoints)):
    input_path = f"s3://postedf1datapipeline/bronze/raw/{endpoints[i]}/{endpoints[i]}.json"
    df = spark.read.option("multiline", "true").json(input_path)
    
    dyf = DynamicFrame.fromDF(df, glueContext, "dyf")
    #clear old data
    empty_s3_path("postedf1datapipeline", f"bronze/structured/{endpoints[i]}/")

    # Step 3: Write data to S3 + update Glue Data Catalog
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": f"s3://postedf1datapipeline/bronze/structured/{endpoints[i]}/",
            "partitionKeys": []  # or ["year", "month"] if you want partitioning
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )












job.commit()