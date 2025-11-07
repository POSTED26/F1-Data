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


input_path = f"s3://postedf1datapipeline/bronze/structured/sessions/"
df = spark.read.parquet(input_path)
df.createOrReplaceTempView('sessions')

sqlDF = spark.sql(
    '''SELECT 
        meeting_key,
        session_key,
        session_name,
        session_type,
        country_name,
        year
    FROM sessions 
    WHERE session_type = 'Race' ''')
    

print(sqlDF.show())

#TODO: Add deequ for data validation checks

dyf = DynamicFrame.fromDF(sqlDF, glueContext, "dyf")
empty_s3_path("postedf1datapipeline", f"silver/sessions/")

glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": f"s3://postedf1datapipeline/silver/sessions/",
            "partitionKeys": []  # or ["year", "month"] if you want partitioning
        },
        format="parquet",
        format_options={"compression": "snappy"}
    )

job.commit()