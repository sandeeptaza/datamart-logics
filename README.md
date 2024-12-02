
# Glue ETL Job for Incremental and Full Data Load

## Overview

This Glue ETL script performs both incremental and full data loads for a set of tables specified in the `table_incremental_fields` dictionary. The script uses AWS Glue, S3, and Apache Spark to handle large-scale data extraction, transformation, and loading tasks. It includes processes for:

1. **Incremental Data Load**: Fetches only new or updated records based on a timestamp column.
2. **Full Data Load**: Loads the complete dataset for all configured tables.
3. **Joins Across Tables**: Merges data from multiple tables into a unified dataset.
4. **Processing Specific Tables**: Handles individual tables (e.g., `adjustment`) with standalone processing logic.
5. **CDC Tracking**: Stores and updates the last load timestamp in S3 for incremental data loads.

---

## Key Features

- **Dynamic Frame Filtering**: Filters tables based on the `updated_at` or `created_at` fields for incremental loading.
- **Error Handling**: Includes logging and exception handling for robust processing.
- **Parameterized Execution**: Accepts runtime parameters for flexibility:
  - `JOB_NAME`: Glue job name.
  - `database_name`: Glue catalog database name.
  - `timestamp_s3_path`: S3 path for storing and retrieving the last load timestamp.
  - `load_type`: Specifies either `full` or `incremental`.

---

## Table Configuration

The following tables are processed by this script:

| Table Name                                         | Incremental Field  |
|---------------------------------------------------|--------------------|
| account_metadata_datamart_public_queue            | `updated_at`       |
| account_metadata_datamart_public_account          | `updated_at`       |
| account_metadata_datamart_public_account_metadata | `updated_at`       |
| account_metadata_datamart_public_settlement_transaction | `created_at` |
| account_metadata_datamart_public_account_risk     | `updated_at`       |
| account_metadata_datamart_public_adjustment       | `updated_at`       |

---

## S3 Path Requirements

- **Timestamp File**: The `timestamp_s3_path` should point to a CSV file with the format:

  ```csv
  last_load_timestamp
  YYYY-MM-DD HH:MM:SS
  ```

- **Output Path**: Data is written to S3 in the following directory structure:

  ```
  s3://iceberg-logics-sandeep/datamart/<database_name>/
  ```

---

## Execution Flow

1. **Parameter Parsing**: Reads job parameters for database name, S3 path, and load type.
2. **Timestamp Retrieval**: Fetches the last load timestamp from S3 or defaults to `None` for a full load.
3. **Data Loading**:
   - For `incremental` load: Filters records based on the last load timestamp.
   - For `full` load: Retrieves all records.
4. **Table Processing**:
   - Processes standalone tables (e.g., `adjustment`).
   - Performs joins across other tables.
5. **Data Writing**:
   - Writes the joined dataset to S3 in Parquet format.
   - Updates the last load timestamp in S3.

---

## Logging

The script uses Python's `logging` library to output logs at various levels:
- `INFO`: General process updates and successes.
- `WARNING`: Missing or empty datasets.
- `ERROR`: Critical issues that halt processing.

---

## Error Handling

- Missing Timestamp File: Defaults to a full load if the timestamp file is not found.
- Empty Data Frames: Skips further operations for empty datasets.
- Missing Tables: Exits the job if any required table is missing for joins.

---

## Prerequisites

1. **AWS Glue Setup**:
   - Ensure the Glue job has necessary IAM permissions for S3, Glue Catalog, and CloudWatch logging.
   - Configure the Glue database and tables in the AWS Glue Data Catalog.
2. **S3 Bucket**:
   - Create the required S3 bucket and folders for storing timestamps and outputs.
3. **Glue Parameters**:
   - Provide the correct runtime parameters during job submission.

---

## Usage

### Submit Job in AWS Glue Console

1. Navigate to the AWS Glue Console.
2. Create a new Glue job and upload the script.
3. Set the required parameters:
   - `--JOB_NAME`: Glue job name.
   - `--database_name`: Glue catalog database name.
   - `--timestamp_s3_path`: S3 path for timestamp file.
   - `--load_type`: `full` or `incremental`.



---

## Known Limitations

1. **Schema Changes**: The script assumes consistent schemas in the Glue Catalog from Tazapay PRD DB.
2. **Performance**: Incremental loading can still process large datasets, which may impact runtime significantly.
3. **Error Handling**: Limited recovery options for missing tables in joins.

---

## Extending the Script

1. **New Tables**: Add entries to the `table_incremental_fields` dictionary.
2. **Custom Logic**: Update functions like `process_adjustment` or `perform_joins` to include new logic.
3. **Alternate Storage**: Modify the `output_path` for different S3 bucket configurations.
