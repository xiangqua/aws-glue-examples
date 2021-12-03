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

# Script generated for node clickhouse
clickhouse_node1635521341311 = glueContext.create_dynamic_frame.from_options(
    connection_type="custom.jdbc",
    connection_options={
        "tableName": "test",
        "dbTable": "test",
        "connectionName": "click",
    },
    transformation_ctx="clickhouse_node1635521341311",
)

# Script generated for node Amazon S3
AmazonS3_node1635521344937 = glueContext.write_dynamic_frame.from_options(
    frame=clickhouse_node1635521341311,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://quandata1/clickhouse/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1635521344937",
)

job.commit()
