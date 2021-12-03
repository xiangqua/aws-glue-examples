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
## @type: DataSource
## @args: [database = "caibo", table_name = "test_caibo", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
#datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "caibo", table_name = "test_caibo", transformation_ctx = "datasource0")

df = glueContext.read.format("jdbc").option("url","jdbc:mysql://database-2.cdsossfija0p.ap-southeast-1.rds.amazonaws.com:3306/test").option("user","admin").option("password","12345678").option("query","select * from caibo where date > '2021-10-18'").option("driver","com.mysql.jdbc.Driver").load()
df.printSchema()
df.show()
df.count()

datasource0 = DynamicFrame.fromDF(df, glueContext, "datasource0")


## @type: ApplyMapping
## @args: [mapping = [("date", "date", "date", "date"), ("id", "string", "id", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("date", "date", "date", "date"), ("id", "string", "id", "string")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://quandata1/caibo1/"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://quandata1/caibo1/"}, format = "csv", transformation_ctx = "datasink2")
job.commit()