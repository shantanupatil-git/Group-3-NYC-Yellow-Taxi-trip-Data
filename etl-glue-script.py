import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import lit, col, when, expr, to_date, date_format, hour
from pyspark.sql.types import IntegerType
from awsglue.dynamicframe import DynamicFrame

# ✅ Accepting dynamic input/output paths from CLI (GitHub Actions)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_PATH', 'TARGET_PATH'])
input_path = args['SOURCE_PATH']
output_path = args['TARGET_PATH']

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ✅ Load datasets dynamically from provided source S3 path
master_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="parquet"
)

# Load zone lookup from fixed location
zone_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://nycfinalp/taxi_zone_lookup.csv"]},
    format="csv",
    format_options={"withHeader": True}
)

# Convert DynamicFrame to DataFrame
master_df = master_dyf.toDF()
zone_df = zone_dyf.toDF()

# Transformations
transformed_df = (master_df
    .filter((col("passenger_count").isNotNull()) & (col("passenger_count") != 0))
    .withColumn("time_of_day",
                when((hour(col("tpep_pickup_datetime")) >= 6) & (hour(col("tpep_pickup_datetime")) < 18), "Day")
                .otherwise("Night"))
    .fillna({'RatecodeID': 99})
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
    .withColumn("RatecodeID", col("RatecodeID").cast(IntegerType()))
    .drop("store_and_fwd_flag", "airport_fee", "congestion_surcharge")
    .join(zone_df.withColumnRenamed("LocationID", "PULocationID")
                 .withColumnRenamed("Zone", "pickup_zone"), on="PULocationID", how="left")
    .join(zone_df.withColumnRenamed("LocationID", "DOLocationID")
                 .withColumnRenamed("Zone", "dropoff_zone"), on="DOLocationID", how="left")
    .withColumn("tip_percentage",
                when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100).otherwise(0))
    .filter((col("trip_distance") <= 100) & (col("fare_amount") <= 500))
    .withColumn("distance_bucket",
                when(col("trip_distance") < 1, "0-1 miles")
                .when((col("trip_distance") >= 1) & (col("trip_distance") < 5), "1-5 miles")
                .when((col("trip_distance") >= 5) & (col("trip_distance") < 10), "5-10 miles")
                .otherwise("10+ miles"))
    .withColumn("payment_type_desc",
                expr("""
                    CASE payment_type
                        WHEN 1 THEN 'Credit Card'
                        WHEN 2 THEN 'Cash'
                        WHEN 3 THEN 'No Charge'
                        WHEN 4 THEN 'Dispute'
                        WHEN 5 THEN 'Unknown'
                        WHEN 6 THEN 'Voided'
                        ELSE 'Other'
                    END
                """))
    .withColumn("ratecode_desc",
                when(col("RatecodeID") == 1, "Standard rate")
                .when(col("RatecodeID") == 2, "JFK")
                .when(col("RatecodeID") == 3, "Newark")
                .when(col("RatecodeID") == 4, "Nassau or Westchester")
                .when(col("RatecodeID") == 5, "Negotiated fare")
                .when(col("RatecodeID") == 6, "Group ride")
                .when(col("RatecodeID") == 99, "Unknown")
                .otherwise("Other"))
    .withColumn("vendor_desc",
                when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
                .when(col("VendorID") == 2, "Curb Mobility, LLC")
                .when(col("VendorID") == 6, "Myle Technologies Inc")
                .when(col("VendorID") == 7, "Helix")
                .when(col("VendorID").isin(3, 4, 5), "Third Party"))
    .drop("service_zone", "borough")
)

# Convert back to DynamicFrame
transformed_dyf = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dyf")

# Write transformed data to dynamically provided S3 path
transformed_dyf = transformed_dyf.repartition(1)
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dyf,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    transformation_ctx="write_output",
    format_options={"compression": "snappy"}
)

# Commit the job
job.commit()
