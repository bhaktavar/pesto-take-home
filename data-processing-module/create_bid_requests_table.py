import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

bid_requests_dyf = glueContext.write_dynamic_frame.from_options(
    frame = applymapping1,
    connection_type = "s3",
    connection_options = {"path": "s3://valid-data-bucket/bid_requests"},
    format = "json",
    format_options = {"jsonPath": "$[*]"},
    transformation_ctx = "bid_requests_dyf"
)

bid_requests_dyf = bid_requests_dyf.drop_duplicates(['bidRequestId'])

def transform_data(record):
    if record['demographic'] not in ['18-24', '25-34', '35-44', '45-54', '55+']:
        return None
    if record['gender'] not in ['Male', 'Female']:
        return None
    if record['device'] not in ['Desktop', 'Mobile', 'Tablet']:
        return None
    if record['operatingSystem'] not in ['iOS', 'Android', 'Windows', 'MacOS', 'Linux']:
        return None
    record['location'] = record['location'].upper()
    record['content_type'] = record['content_type'].title()
    return record

bid_requests_rdd = bid_requests_dyf.rdd.map(transform_data).filter(lambda x: x is not None)

bid_requests_df = spark.createDataFrame(bid_requests_rdd)

bid_requests_df.write \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://pesto_redshift_cluster:8192/pesto_database") \
    .option("dbtable", "bid_requests") \
    .option("tempdir", "s3://pesto-temp-dir/") \
    .option("user", "rs_admin") \
    .option("password", "password1234") \
    .option("aws_iam_role", "arn:aws:iam::1234567890:role/redshift_connection_role") \
    .mode("append") \
    .save()

print("Bid Requests data loaded into Redshift successfully!")
