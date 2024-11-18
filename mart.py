"""
Author: Sandeep R Diddi
Date: 2024-11-18
"""

import sys
import logging
import pandas as pd
import boto3
from io import StringIO
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Glue context and parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database_name', 'timestamp_s3_path'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

database_name = args['database_name']
timestamp_s3_path = args['timestamp_s3_path']

# Initialize S3 client
s3 = boto3.client('s3')

def get_last_load_timestamp(s3_path):
    try:
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        csv_obj = s3.get_object(Bucket=bucket, Key=key)
        last_timestamp_df = pd.read_csv(csv_obj['Body'])
        last_timestamp = last_timestamp_df.iloc[0, 0]
        logger.info(f"Last load timestamp retrieved: {last_timestamp}")
        return last_timestamp
    except s3.exceptions.NoSuchKey:
        logger.info("No previous timestamp found. Running full load.")
        return None  # No previous timestamp found
    except Exception as e:
        logger.error(f"Error reading last load timestamp: {str(e)}")
        raise

def update_last_load_timestamp(s3_path, timestamp):
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    csv_buffer = StringIO()
    pd.DataFrame([timestamp], columns=["last_load_timestamp"]).to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    logger.info(f"Updated last load timestamp to {timestamp} on S3.")

try:
    # Get last load timestamp from S3
    last_timestamp = get_last_load_timestamp(timestamp_s3_path)
    load_type = 'full' if last_timestamp is None else 'incremental'

    # Retrieve all tables from the Glue catalog for the specified database
    tables = glueContext.catalog().get_tables(database_name)
    if not tables:
        logger.error(f"No tables found in database '{database_name}'. Exiting job.")
        sys.exit(1)
    
    logger.info(f"Found {len(tables)} tables in the database '{database_name}'")

    # Dictionary to hold the dynamic frames for each table
    dynamic_frames = {}

    # Load each table as a dynamic frame
    for table in tables:
        table_name = table.name
        try:
            logger.info(f"Loading table '{table_name}'")
            dynamic_frame = glueContext.create_dynamic_frame.from_catalog(database=database_name, table_name=table_name)

            if load_type == 'incremental':
                # Apply filter for incremental load based on last_timestamp
                dynamic_frame = Filter.apply(frame=dynamic_frame, f=lambda row: row["updated_at"] > last_timestamp)
                logger.info(f"Filtered table '{table_name}' for incremental load")

            dynamic_frames[table_name] = dynamic_frame
        except Exception as e:
            logger.error(f"Error loading table '{table_name}': {str(e)}")
            continue  # Skip this table and move to the next

    # Join all dynamic frames
    joined_df = None
    for i, (table_name, dynamic_frame) in enumerate(dynamic_frames.items()):
        if i == 0:
            joined_df = dynamic_frame  # Initialize with the first table
        else:
            try:
                # Adjust join key based on your requirements
                joined_df = Join.apply(joined_df, dynamic_frame, 'account_id', 'account_id')
                logger.info(f"Joined '{table_name}' successfully")
            except Exception as e:
                logger.error(f"Error joining table '{table_name}': {str(e)}")
                continue  # Skip this table and move to the next join

    if joined_df is None:
        logger.error("No joined data frame was created. Exiting job.")
        sys.exit(1)

    # Write the result to S3
    output_path = f"s3://iceberg-logics-sandeep/datamart/{database_name}/"
    logger.info(f"Writing joined data to S3 path: {output_path}")

    glueContext.write_dynamic_frame.from_options(frame=joined_df, 
                                                 connection_type="s3", 
                                                 connection_options={"path": output_path}, 
                                                 format="parquet")

    # Update last load timestamp in S3 with the maximum `updated_at` value from joined data
    max_timestamp = joined_df.toDF().agg({"updated_at": "max"}).collect()[0][0]
    if max_timestamp:
        update_last_load_timestamp(timestamp_s3_path, max_timestamp)

    logger.info("Job completed successfully.")
    job.commit()

except Exception as e:
    logger.error(f"Job failed with error: {str(e)}")
    job.commit()
    raise
