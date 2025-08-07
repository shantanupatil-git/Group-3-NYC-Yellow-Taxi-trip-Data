import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import lit, col, when, expr, to_date, date_format, year, dayofmonth, floor, row_number, hour
from pyspark.sql.types import IntegerType
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load datasets
master_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://nycfinalp/final-merged-copy/part-00000-a9b37a2c-b51f-4a69-9db2-bffec381dc6c-c000.snappy.parquet"]},
    format="parquet"
)

zone_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://nycfinalp/taxi_zone_lookup.csv"]},
    format="csv",
    format_options={"withHeader": True}
)

# Convert DynamicFrame to DataFrame
master_df = master_dyf.toDF()
zone_df = zone_dyf.toDF()

# Data transformations
transformed_df = (master_df
    # Filter by year (2019-2022)
    .filter((year("tpep_pickup_datetime") >= 2019) & (year("tpep_pickup_datetime") <= 2022))
    
    # Cast timestamp
    .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
    
    # Add week of month
    .withColumn("week_of_month", (floor((dayofmonth("tpep_pickup_datetime") - 1) / 7) + 1).cast("int"))
    
    # Add day name
    .withColumn("day_name", date_format("tpep_pickup_datetime", "EEEE"))
    
    # Filter out null or zero passenger_count
    .filter((col("passenger_count").isNotNull()) & (col("passenger_count") != 0))
    
    # Add time of day
    .withColumn("time_of_day",
                when((hour(col("tpep_pickup_datetime")) >= 6) & (hour(col("tpep_pickup_datetime")) < 18), "Day")
                .otherwise("Night"))
    
    # Fill missing RatecodeID and cast types
    .fillna({'RatecodeID': 99})
    .withColumn("passenger_count", col("passenger_count").cast(IntegerType()))
    .withColumn("RatecodeID", col("RatecodeID").cast(IntegerType()))
    
    # Drop unused columns
    .drop("store_and_fwd_flag", "airport_fee", "congestion_surcharge")
    
    # Join with zone lookup tables
    .join(zone_df.select(col("LocationID").alias("PULocationID"),
                        col("Zone").alias("pickup_zone"),
                        col("Borough").alias("pickup_borough")),
          on="PULocationID", how="left")
    .join(zone_df.select(col("LocationID").alias("DOLocationID"),
                        col("Zone").alias("dropoff_zone"),
                        col("Borough").alias("dropoff_borough")),
          on="DOLocationID", how="left")
    
    # Add row number as Id
    .withColumn("Id", row_number().over(Window.orderBy(lit(1))))
    
    # Add tip percentage
    .withColumn("tip_percentage",
                when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100)
                .otherwise(0))
    
    # Filter extreme values
    .filter((col("trip_distance") <= 100) & (col("fare_amount") <= 500))
    
    # Add distance bucket
    .withColumn("distance_bucket",
                when(col("trip_distance") < 1, "0-1 miles")
                .when((col("trip_distance") >= 1) & (col("trip_distance") < 5), "1-5 miles")
                .when((col("trip_distance") >= 5) & (col("trip_distance") < 10), "5-10 miles")
                .otherwise("10+ miles"))
    
    # Map payment type
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
    
    # Map RatecodeID
    .withColumn("ratecode_desc",
                when(col("RatecodeID") == 1, "Standard rate")
                .when(col("RatecodeID") == 2, "JFK")
                .when(col("RatecodeID") == 3, "Newark")
                .when(col("RatecodeID") == 4, "Nassau or Westchester")
                .when(col("RatecodeID") == 5, "Negotiated fare")
                .when(col("RatecodeID") == 6, "Group ride")
                .when(col("RatecodeID") == 99, "Unknown")
                .otherwise("Other"))
    
    # Map VendorID
    .withColumn("vendor_desc",
                when(col("VendorID") == 1, "Creative Mobile Technologies, LLC")
                .when(col("VendorID") == 2, "Curb Mobility, LLC")
                .when(col("VendorID") == 6, "Myle Technologies Inc")
                .when(col("VendorID") == 7, "Helix")
                .when(col("VendorID").isin(3, 4, 5), "Third Party"))
    
    # Drop unnecessary columns
    .drop("service_zone")
)

# Convert back to DynamicFrame
transformed_dyf = DynamicFrame.fromDF(transformed_df, glueContext, "transformed_dyf")

# Write output to S3
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dyf,
    connection_type="s3",
    connection_options={"path": "s3://nycfinalp/testing-master-data/"},
    format="parquet",
    transformation_ctx="write_output",
    format_options={"compression": "snappy"}
)

# Commit the job 
job.commit()