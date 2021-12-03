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
        "parentProject": "mimetic-campus-318604",
        "table": "mimetic-campus-318604.analytics_276203953.test_view",
        "viewsEnabled": "true",
        "connectionName": "bigquery",
    },
    transformation_ctx="AWSGlueConnectorforGoogleBigQuery_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=AWSGlueConnectorforGoogleBigQuery_node1,
    mappings=[("user_pseudo_id", "string", "user_pseudo_id", "string"), ("event_date", "string", "event_date", "string"), ("path", "string", "path", "string")],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon Redshift
#AmazonRedshift_node1634868522205 = glueContext.write_dynamic_frame.from_catalog(
#    frame= ApplyMapping_node2,
#    database="bigquery",
#    table_name="bigquery_public_cux_checkout_paths",
#    redshift_tmp_dir=args["TempDir"],
#    transformation_ctx="AmazonRedshift_node1634868522205",
#)

AmazonRedshift_node1634868522205 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = ApplyMapping_node2, 
    catalog_connection = "redshift", 
    connection_options = {"dbtable": "cux_checkout_paths", "database": "dev"}, 
    redshift_tmp_dir = args["TempDir"], 
    transformation_ctx = "AmazonRedshift_node1634868522205",
)

job.commit()
