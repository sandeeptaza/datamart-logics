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

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'database_name', 'timestamp_s3_path', 'load_type'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

database_name = args['database_name']
timestamp_s3_path = args['timestamp_s3_path']
load_type = args['load_type']

# Initialize S3 client
s3 = boto3.client('s3')

# Table configuration for incremental fields
table_incremental_fields = {
    "account_metadata_datamart_public_account": "updated_at",
    "account_metadata_datamart_public_account_metadata": "updated_at",
    "account_metadata_datamart_public_account_risk": "updated_at",
    "account_metadata_datamart_public_queue": "updated_at",
    "account_metadata_datamart_public_settlement_transaction": "created_at"  
}

# Function to get the last processed timestamp from S3
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
        return None
    except Exception as e:
        logger.error(f"Error reading last load timestamp: {str(e)}")
        raise

# Function to update the timestamp in S3
def update_last_load_timestamp(s3_path, timestamp):
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    csv_buffer = StringIO()
    pd.DataFrame([timestamp], columns=["last_load_timestamp"]).to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    logger.info(f"Updated last load timestamp to {timestamp} on S3.")

# Function to load data incrementally
def load_incremental_data(last_timestamp):
    dynamic_frames = {}
    for table_name, incremental_field in table_incremental_fields.items():
        try:
            logger.info(f"Processing table: {table_name}")
            dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
                database=database_name, table_name=table_name
            )

            # Log schema for debugging
            schema_fields = [field.name for field in dynamic_frame.schema().fields]
            logger.info(f"Schema for table {table_name}: {schema_fields}")

            # Verify column existence
            if incremental_field not in schema_fields:
                logger.warning(f"Column '{incremental_field}' not found in {table_name}. Skipping.")
                continue

            # Apply incremental filter if timestamp exists
            if last_timestamp:
                logger.info(f"Filtering records where {incremental_field} > {last_timestamp}")
                logger.info(f"Row count before filtering: {dynamic_frame.count()}")
                dynamic_frame = Filter.apply(
                    frame=dynamic_frame,
                    f=lambda row: row[incremental_field] and row[incremental_field] > last_timestamp
                )
                logger.info(f"Row count after filtering: {dynamic_frame.count()}")

            if dynamic_frame.count() > 0:
                dynamic_frames[table_name] = dynamic_frame
                logger.info(f"Successfully loaded table: {table_name} with {dynamic_frame.count()} rows.")
            else:
                logger.warning(f"No records found for table {table_name} after filtering. Skipping.")
        except Exception as e:
            logger.error(f"Error processing table {table_name}: {str(e)}")
            continue
    return dynamic_frames

# Function to load full data (initial load)
def load_full_data():
    dynamic_frames = {}
    for table_name in table_incremental_fields.keys():
        try:
            logger.info(f"Loading full data for table: {table_name}")
            dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
                database=database_name, table_name=table_name
            )

            if dynamic_frame.count() > 0:
                dynamic_frames[table_name] = dynamic_frame
                logger.info(f"Successfully loaded table: {table_name} with {dynamic_frame.count()} rows.")
            else:
                logger.warning(f"No records found for table {table_name}. Skipping.")
        except Exception as e:
            logger.error(f"Error loading full data for table {table_name}: {str(e)}")
            continue
    return dynamic_frames

# Function to perform joins
def perform_joins(dynamic_frames):
    try:
        logger.info(f"Loaded tables: {list(dynamic_frames.keys())}")
        base_table = "account_metadata_datamart_public_account"  # Corrected base table name from Glue Catlouge in Taza DEMO Env
        if base_table not in dynamic_frames:
            logger.error(f"Base table {base_table} is missing. Cannot perform joins.")
            sys.exit(1)

        joined_df = dynamic_frames[base_table]
        logger.info(f"Initial row count for base table: {joined_df.count()}")

        # Join account_metadata
        if "account_metadata_datamart_public_account_metadata" in dynamic_frames:
            joined_df = Join.apply(joined_df, dynamic_frames["account_metadata_datamart_public_account_metadata"], "account_id", "account_id")
            logger.info(f"Row count after joining with account_metadata: {joined_df.count()}")

        # Join account_risk
        if "account_metadata_datamart_public_account_risk" in dynamic_frames:
            joined_df = Join.apply(joined_df, dynamic_frames["account_metadata_datamart_public_account_risk"], "account_id", "account_id")
            logger.info(f"Row count after joining with account_risk: {joined_df.count()}")

        # Join queue
        if "account_metadata_datamart_public_queue" in dynamic_frames:
            joined_df = Join.apply(joined_df, dynamic_frames["account_metadata_datamart_public_queue"], "account_id", "account_id")
            logger.info(f"Row count after joining with queue: {joined_df.count()}")

        # Join settlement_transaction
        if "account_metadata_datamart_public_settlement_transaction" in dynamic_frames:
            joined_df = Join.apply(joined_df, dynamic_frames["account_metadata_datamart_public_settlement_transaction"], "queue_id", "queue_id")
            logger.info(f"Row count after joining with settlement_transaction: {joined_df.count()}")

        return joined_df
    except Exception as e:
        logger.error(f"Error joining tables: {str(e)}")
        sys.exit(1)

# Main execution
try:
    last_timestamp = get_last_load_timestamp(timestamp_s3_path)
    load_type = 'full' if not last_timestamp or load_type == 'full' else 'incremental'

    if load_type == 'incremental':
        logger.info("Running incremental update...")
        dynamic_frames = load_incremental_data(last_timestamp)
    else:
        logger.info("Running full load...")
        dynamic_frames = load_full_data()

    joined_df = perform_joins(dynamic_frames)

    if joined_df and joined_df.count() > 0:
        output_path = f"s3://iceberg-logics-sandeep/datamart/{database_name}/"
        glueContext.write_dynamic_frame.from_options(
            frame=joined_df,
            connection_type="s3",
            connection_options={"path": output_path},
            format="parquet"
        )
        logger.info(f"Data successfully written to {output_path}")
    else:
        logger.warning("Joined dataset is empty or invalid. Skipping write operation.")

    max_timestamp = max(
        frame.toDF().agg({table_incremental_fields[table]: "max"}).collect()[0][0]
        for table, frame in dynamic_frames.items()
    )
    if max_timestamp:
        update_last_load_timestamp(timestamp_s3_path, max_timestamp)

    logger.info("Job completed successfully.")
    job.commit()

except Exception as e:
    logger.error(f"Job failed with error: {str(e)}")
    job.commit()
    raise