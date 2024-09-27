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

# Script generated for node Customer Trusted
CustomerTrusted_node1727281720535 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1727281720535")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1727281689849 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1727281689849")

# Script generated for node Join
Join_node1727281735052 = Join.apply(frame1=CustomerTrusted_node1727281720535, frame2=AccelerometerLanding_node1727281689849, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1727281735052")

# Script generated for node Drop fields
SqlQuery1152 = '''
select distinct
customername,email,phone,birthday,
serialnumber,registrationdate,
lastupdatedate,sharewithresearchasofdate,sharewithpublicasofdate,
sharewithfriendsasofdate
from myDataSource
where sharewithresearchasofdate is not null
;
'''
Dropfields_node1727282636644 = sparkSqlQuery(glueContext, query = SqlQuery1152, mapping = {"myDataSource":Join_node1727281735052}, transformation_ctx = "Dropfields_node1727282636644")

# Script generated for node Customer_Curated
Customer_Curated_node1727282312962 = glueContext.getSink(path="s3://matanbucket1/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customer_Curated_node1727282312962")
Customer_Curated_node1727282312962.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_curated")
Customer_Curated_node1727282312962.setFormat("json")
Customer_Curated_node1727282312962.writeFrame(Dropfields_node1727282636644)
job.commit()