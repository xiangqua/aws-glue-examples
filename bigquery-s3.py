import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Connector for Google BigQuery
AWSGlueConnectorforGoogleBigQuery_node1 = glueContext.create_dynamic_frame.from_options(
    connection_type="marketplace.spark",
    connection_options={
        "parentProject": "elated-lotus-333913",
        "table": "elated-lotus-333913.bq.test_v",
        "viewsEnabled": "true",
        "connectionName": "bq",
    },
    transformation_ctx="AWSGlueConnectorforGoogleBigQuery_node1",
)


# Script generated for node Amazon S3
AmazonS3_node1638456458201 = glueContext.write_dynamic_frame.from_options(
    frame=AWSGlueConnectorforGoogleBigQuery_node1,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://quandata1/glue/bigquery/1/"},
    transformation_ctx="AmazonS3_node1638456458201",
)

job.commit()
