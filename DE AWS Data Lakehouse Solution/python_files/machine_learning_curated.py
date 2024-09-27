import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_curated
customer_curated_node1727473768210 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_curated", transformation_ctx="customer_curated_node1727473768210")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1727473083693 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1727473083693")

# Script generated for node accelerometer_landing
accelerometer_landing_node1727473101280 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1727473101280")

# Script generated for node JOIN
SqlQuery1380 = '''
select 
s.sensorreadingtime,a.*
from step_trainer_trusted s
join customer_curated c on 1=1
and s.serialnumber = c.serialnumber
join accelerometer_landing a
on 1=1
and s.sensorreadingtime = a.timestamp
'''
JOIN_node1727473718422 = sparkSqlQuery(glueContext, query = SqlQuery1380, mapping = {"accelerometer_landing":accelerometer_landing_node1727473101280, "step_trainer_trusted":step_trainer_trusted_node1727473083693, "customer_curated":customer_curated_node1727473768210}, transformation_ctx = "JOIN_node1727473718422")

# Script generated for node machine_learning_curated
machine_learning_curated_node1727473153387 = glueContext.getSink(path="s3://matanbucket1/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1727473153387")
machine_learning_curated_node1727473153387.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
machine_learning_curated_node1727473153387.setFormat("json")
machine_learning_curated_node1727473153387.writeFrame(JOIN_node1727473718422)
job.commit()