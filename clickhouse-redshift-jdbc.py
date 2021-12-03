import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


db_url = "jdbc:clickhouse://172.31.27.24:8123/test"
db_driver = "ru.yandex.clickhouse.ClickHouseDriver"
db_user = "default"
db_password = "123456"

tables = [
    "test",
]

for table in tables:

    # Connecting to the source
    df = (
        spark.read.format("jdbc")
        .option("url", db_url)
        .option("dbtable", table)
        .option("driver", db_driver)
        .option("user", db_user)
        .option("password", db_password)
        .load()
    )

    # Save to S3 with overwrite data
    #df.write.mode("overwrite").format(
    #    "csv"
    #).save(
    #    f"s3://quandata1/clickhouse/{table}/"
    #)
    
    datasource0 = DynamicFrame.fromDF(df, glueContext, "datasource0")
    datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = datasource0, catalog_connection = "myredshift", connection_options = {"dbtable": "redshift_test", "database": "red"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()