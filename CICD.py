import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

# Function for Spark SQL Queries
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = glueContext.spark_session.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Arguments and Job Initialization
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Step 1: Read data from the Glue Data Catalog (S3 Source)
insurance_data = glueContext.create_dynamic_frame.from_catalog(
    database="testdbjul2024",
    table_name="insurance_csv",
    transformation_ctx="insurance_data",
)

# Step 2: Drop Null Rows
filtered_data = insurance_data.toDF().dropna(subset=["patient_id", "insurance_company", "policy_number"])
filtered_data = DynamicFrame.fromDF(filtered_data, glueContext, "filtered_data")

# Step 3: SQL Query to Process Data
sql_query = """
SELECT 
    patient_id,
    insurance_company,
    policy_number,
    ROUND(coverage_amount, 2) AS coverage_amount,
    ROUND(insurance_cost, 2) AS insurance_cost
FROM insurance_data
"""
processed_data = sparkSqlQuery(
    glueContext,
    query=sql_query,
    mapping={"insurance_data": filtered_data},
    transformation_ctx="processed_data",
)

# Step 4: Write Data to S3
glueContext.write_dynamic_frame.from_options(
    frame=processed_data,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://bdjul2024/silver/",
        "partitionKeys": [],  # Add partition keys if required
    },
    transformation_ctx="write_to_s3",
)

# Commit Job
job.commit()
