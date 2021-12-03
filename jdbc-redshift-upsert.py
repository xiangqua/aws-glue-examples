import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "glue", table_name = "glue_item", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []

yesterday = '2021-10-18'


add_info= {"hashexpression":"update_date = '" + yesterday + "' AND update_date","hashpartitions":"10"}


datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "glue", table_name = "glue_item", additional_options = add_info, transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("price", "int", "price", "int"), ("update_date", "date", "update_date", "date"), ("item_id", "int", "item_id", "int"), ("item_name", "string", "item_name", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("price", "int", "price", "int"), ("update_date", "date", "update_date", "date"), ("item_id", "int", "item_id", "int"), ("item_name", "string", "item_name", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")
## @type: DataSink
## @args: [catalog_connection = "myredshift", connection_options = {"dbtable": "glue_item", "database": "test"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]

post_query="begin;delete from glue_item using stage_table where stage_table.item_id = glue_item.item_id ; insert into glue_item select * from stage_table; drop table stage_table; end;"
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields3, catalog_connection = "myredshift", connection_options = {"preactions":"drop table if exists stage_table;create table stage_table as select * from glue_item where 1=2;", "dbtable": "stage_table", "database": "test","postactions":post_query}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()