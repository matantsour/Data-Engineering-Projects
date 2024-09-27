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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1727470590179 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1727470590179")

# Script generated for node Customer Curated
CustomerCurated_node1727470644336 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_curated", transformation_ctx="CustomerCurated_node1727470644336")

# Script generated for node Joins and Drop fields
SqlQuery1164 = '''
select s.*
from step_trainer_landing s
join customer_curated c on 1=1
and s.serialnumber = c.serialnumber
where c.sharewithresearchasofdate is not null
'''
JoinsandDropfields_node1727471391000 = sparkSqlQuery(glueContext, query = SqlQuery1164, mapping = {"step_trainer_landing":StepTrainerLanding_node1727470590179, "customer_curated":CustomerCurated_node1727470644336}, transformation_ctx = "JoinsandDropfields_node1727471391000")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1727471467351 = glueContext.getSink(path="s3://matanbucket1/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1727471467351")
step_trainer_trusted_node1727471467351.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1727471467351.setFormat("json")
step_trainer_trusted_node1727471467351.writeFrame(JoinsandDropfields_node1727471391000)
job.commit()