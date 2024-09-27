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

# Script generated for node Customer Landing
CustomerLanding_node1727174922313 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="customer_landing", transformation_ctx="CustomerLanding_node1727174922313")

# Script generated for node Share with Research
SqlQuery1291 = '''
select * from myDataSource
where 	
sharewithresearchasofdate is not  null
'''
SharewithResearch_node1727175026421 = sparkSqlQuery(glueContext, query = SqlQuery1291, mapping = {"myDataSource":CustomerLanding_node1727174922313}, transformation_ctx = "SharewithResearch_node1727175026421")

# Script generated for node Customer Trusted
CustomerTrusted_node1727182796543 = glueContext.getSink(path="s3://matanbucket1/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1727182796543")
CustomerTrusted_node1727182796543.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
CustomerTrusted_node1727182796543.setFormat("json")
CustomerTrusted_node1727182796543.writeFrame(SharewithResearch_node1727175026421)
job.commit()