import boto3
import logging

from pyspark.context import SparkContext
from awsglue.context import GlueContext

logger = logging.getLogger("job")
logger.setLevel(logging.INFO)

logger.info("Job Execution Started…")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


db_url = "jdbc:clickhouse://172.31.27.24:8123/test"
db_driver = "ru.yandex.clickhouse.ClickHouseDriver"
db_user = "default"
db_password = "123456"

tables = [
    "test",
]

for table in tables:
    logger.info(f"Start loading data from {table}...")

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
    df.write.mode("overwrite").format(
        "csv"
    ).save(
        f"s3://quandata1/clickhouse/{table}/"
    )
    logger.info(f"Finish loading data from {table}")