import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job arguments (including JOB_NAME)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from S3 into a Glue DynamicFrame
input_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://bdus829/Ankita/raw_data_2/2ef21156d2b94ac4bdacee955b7ae411.csv"], "header": "true"},
    format="csv"
)

print(input_data.count())
# Check the schema of the DynamicFrame
input_data.printSchema()

# Preview the first few records
input_data.show(10)
# Perform a simple transformation (e.g., filter out records where 'id' is less than 10)
filtered_data = input_data.filter(lambda x: x["id"] >= 10)
print(filtered_data.count())

# Write transformed data back to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    filtered_data,
    connection_type="s3",
    connection_options={"path": "s3://bdus829/Ankita/output_glue/"},
    format="parquet"
)

# Commit the job
job.commit()
