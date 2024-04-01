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

ad_impressions_dyf = glueContext.write_dynamic_frame.from_options(
    frame = applymapping1,
    connection_type = "s3",
    connection_options = {"path": "s3://valid-data-bucket/ad_impressions"},
    format = "json",
    format_options = {"jsonPath": "$[*]"},
    transformation_ctx = "ad_impressions_dyf"
)

clicks_and_conversions_dyf = clicks_and_conversions_dyf.dropDuplicates(['timestamp', 'userId', 'campaignId', 'actionType'])

def transform_data(record):
    if not all(key in record for key in ['adId', 'userId', 'campaignId', 'timestamp', 'website']):
        return None

    record['timestamp'] = datetime.strptime(record['timestamp'], "%Y-%m-%dT%H:%M:%SZ").isoformat()
    return record

ad_impressions_rdd = ad_impressions_dyf.rdd.map(transform_data).filter(lambda x: x is not None)

ad_impressions_df = spark.createDataFrame(ad_impressions_rdd)

ad_impressions_df.write \
    .format("com.databricks.spark.redshift") \
    .option("url", "jdbc:redshift://pesto_redshift_cluster:8192/pesto_database") \
    .option("dbtable", "ad_impressions") \
    .option("tempdir", "s3://pesto-temp-dir/") \
    .option("user", "rs_admin") \
    .option("password", "password1234") \
    .option("aws_iam_role", "arn:aws:iam::1234567890:role/redshift_connection_role") \
    .mode("append") \
    .save()

print("Ad impressions data loaded into Redshift successfully!")
