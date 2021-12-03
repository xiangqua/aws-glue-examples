import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, date, timedelta

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "glue", table_name = "glue_paylog", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []

#today=str(date.today())
#yesterday = str(date.today() + timedelta(days = -1))
yesterday = '2021-10-17'

#print(today)
#print(yesterday)
#add_info= {"hashexpression":"payment_date < '2021-10-19' AND payment_date","hashpartitions":"10"}

add_info= {"hashexpression":"payment_date = '" + yesterday + "' AND payment_date","hashpartitions":"10"}

datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "glue", table_name = "glue_paylog", additional_options = add_info, transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("order_id", "int", "order_id", "int"), ("amount", "int", "amount", "int"), ("trade_no", "string", "trade_no", "string"), ("payment_date", "date", "payment_date", "date")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("order_id", "int", "order_id", "int"), ("amount", "int", "amount", "int"), ("trade_no", "string", "trade_no", "string"), ("payment_date", "date", "payment_date", "date")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://quandata1/glue/bookmark/paylog/"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://quandata1/glue/bookmark/paylog/"}, format = "csv", transformation_ctx = "datasink2")
job.commit()