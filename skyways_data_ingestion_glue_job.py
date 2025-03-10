import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node airport_dim
airport_dim_node1741579451154 = glueContext.create_dynamic_frame.from_catalog(database="skyways_datamart", table_name="dev_skyways_airports_dim",redshift_tmp_dir="s3://skyways-data-landing-zn/redshift-temp-data-gds/skyways-dim/", transformation_ctx="airport_dim_node1741579451154")

# Script generated for node daily_flights_data
daily_flights_data_node1741579204377 = glueContext.create_dynamic_frame.from_catalog(database="skyways_datamart", table_name="raw_daily_flights",  transformation_ctx="daily_flights_data_node1741579204377")

# Script generated for node Filter
Filter_node1741579300425 = Filter.apply(frame=daily_flights_data_node1741579204377, f=lambda row: (row["depdelay"] >= 60), transformation_ctx="Filter_node1741579300425")

# Script generated for node Join
Filter_node1741579300425DF = Filter_node1741579300425.toDF()
airport_dim_node1741579451154DF = airport_dim_node1741579451154.toDF()
Join_node1741579629050 = DynamicFrame.fromDF(Filter_node1741579300425DF.join(airport_dim_node1741579451154DF, (Filter_node1741579300425DF['originairportid'] == airport_dim_node1741579451154DF['airport_id']), "left"), glueContext, "Join_node1741579629050")

# Script generated for node modify_depy_airport_columns
modify_depy_airport_columns_node1741579697717 = ApplyMapping.apply(frame=Join_node1741579629050, mappings=[("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("depdelay", "long", "dep_delay", "bigint"), ("arrdelay", "long", "arr_delay", "bigint"), ("city", "string", "dep_city", "string"), ("name", "string", "dep_airport", "string"), ("state", "string", "dep_state", "string")], transformation_ctx="modify_depy_airport_columns_node1741579697717")

# Script generated for node Join_for_arr_airport
modify_depy_airport_columns_node1741579697717DF = modify_depy_airport_columns_node1741579697717.toDF()
airport_dim_node1741579451154DF = airport_dim_node1741579451154.toDF()
Join_for_arr_airport_node1741580653066 = DynamicFrame.fromDF(modify_depy_airport_columns_node1741579697717DF.join(airport_dim_node1741579451154DF, (modify_depy_airport_columns_node1741579697717DF['destairportid'] == airport_dim_node1741579451154DF['airport_id']), "left"), glueContext, "Join_for_arr_airport_node1741580653066")

# Script generated for node modify_arr_airport_columns
modify_arr_airport_columns_node1741580714633 = ApplyMapping.apply(frame=Join_for_arr_airport_node1741580653066, mappings=[("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("dep_delay", "bigint", "dep_delay", "long"), ("arr_delay", "bigint", "arr_delay", "long"), ("dep_city", "string", "dep_city", "string"), ("dep_airport", "string", "dep_airport", "string"), ("dep_state", "string", "dep_state", "string"), ("airport_id", "long", "airport_id", "long"), ("city", "string", "arr_city", "string"), ("name", "string", "arr_airport", "string"), ("state", "string", "arr_state", "string")], transformation_ctx="modify_arr_airport_columns_node1741580714633")

# Script generated for node redshift_target_table
redshift_target_table_node1741580824225 = glueContext.write_dynamic_frame.from_catalog(frame=modify_arr_airport_columns_node1741580714633, database="skyways_datamart", table_name="dev_skyways_daily_flights_fact", redshift_tmp_dir="s3://skyways-data-landing-zn/redshift-temp-data-gds/skyways-fact/",additional_options={"aws_iam_role": "arn:aws:iam::651706776121:role/redshift-glue-jdbc"}, transformation_ctx="redshift_target_table_node1741580824225")

job.commit()