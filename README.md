
# Glue Job for Incremental and Full Data Loads with S3 Timestamp Tracking

## Overview

This AWS Glue job performs **full** or **incremental data loads** from a Glue Catalog database into S3. It uses a timestamp stored in S3 to determine whether to load all data (full load) or only updated data since the last load (incremental load). The final output is written to S3 in **Parquet format**, and the timestamp is updated in S3 for future runs.

---

## Features

1. **Full and Incremental Data Loading**:
   - Automatically decides between full and incremental loads based on the availability of a timestamp file in S3.
   - Applies filters to only load records updated after the last successful run during incremental loads.

2. **Dynamic Glue Catalog Integration**:
   - Automatically retrieves all tables from the specified Glue database.
   - Loads tables dynamically into AWS Glue Dynamic Frames.

3. **Table Joining**:
   - Joins data from all tables on a common key (`account_id`) into a unified dataset.

4. **Output Management**:
   - Writes the processed dataset to a specified S3 path in **Parquet format**.

5. **Timestamp Management**:
   - Reads and updates the timestamp stored in S3 to ensure incremental loads.

6. **Error Handling**:
   - Logs errors encountered while loading or joining tables and skips problematic tables to ensure job continuity.

---

## Prerequisites

1. **AWS Glue Database**: A Glue catalog database containing the tables to be processed.
2. **S3 Bucket**: A bucket to store the timestamp file and the final output.
3. **IAM Role**: Ensure the Glue job has permissions to access Glue Catalog and the specified S3 bucket.
4. **AWS Glue Job Parameters**:
   - `JOB_NAME`: Name of the Glue job.
   - `database_name`: The Glue catalog database name.
   - `timestamp_s3_path`: The S3 path for the timestamp file (e.g., `s3://your-bucket/last_load_timestamp.csv`).

---

## Code Walkthrough

### 1. **Initialization**
- The Glue context, Spark context, and job parameters are initialized.
- Parameters include:
  - `database_name`: Specifies the Glue database to retrieve tables from.
  - `timestamp_s3_path`: S3 location of the timestamp file for tracking the last successful load.

### 2. **Timestamp Handling**
- **Retrieve Last Load Timestamp**:
  - Reads the timestamp file from S3.
  - If the file does not exist, assumes a full load.
- **Update Last Load Timestamp**:
  - Writes the maximum `updated_at` value from the loaded dataset back to S3.

### 3. **Load Tables into Dynamic Frames**
- Dynamically retrieves all tables from the specified database.
- For each table:
  - Applies a filter for incremental loads (records with `updated_at > last_timestamp`).
  - Handles errors gracefully, skipping problematic tables.

### 4. **Join Tables**
- Iteratively joins all dynamic frames on the key `account_id`.
- Skips any tables that fail during the join process.

### 5. **Write Output to S3**
- Writes the final joined dataset to S3 in Parquet format.
- Updates the timestamp file with the latest `updated_at` value.

### 6. **Error Handling**
- Captures and logs errors at every stage (loading, filtering, joining).
- Ensures the job commits its progress even if some tables or joins fail.

---

## Input Parameters

| Parameter          | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `JOB_NAME`          | Name of the Glue job.                                                      |
| `database_name`     | Name of the Glue catalog database to process.                              |
| `timestamp_s3_path` | S3 path of the CSV file storing the last load timestamp.                   |

---

## Example Use Case

**Scenario**:  
You have a Glue database named `datamart`, with tables containing an `updated_at` column. You want to load and process these tables incrementally, joining them on a common key (`account_id`) and saving the results to S3.

**Steps**:
1. **Run the job for the first time**:
   - No timestamp file exists, so the job performs a **full load**.
2. **Subsequent runs**:
   - The job reads the last timestamp from S3 and performs an **incremental load**, processing only new or updated records.

---

## Output

1. **Processed Data**:  
   - Written to the specified S3 path in Parquet format.

2. **Timestamp File**:  
   - Updated in the S3 path with the latest `updated_at` value.

---

## Logging and Error Handling

1. **Logging**:
   - Logs progress at every step: table loading, filtering, joining, and writing.
2. **Error Handling**:
   - Skips problematic tables or joins and continues processing other tables.
   - Logs errors for debugging.

---

## Example Command

```bash
aws glue start-job-run     --job-name my-glue-job     --arguments '--JOB_NAME=my-glue-job,--database_name=datamart,--timestamp_s3_path=s3://my-bucket/last_load_timestamp.csv'
```

---

## Potential Enhancements

1. **Custom Join Keys**: Modify the code to handle varying join keys for different tables.
2. **Schema Validation**: Add schema checks to ensure compatibility during joins.
3. **Data Quality Checks**: Incorporate checks for null or invalid values before joining.

---

This README provides a comprehensive explanation of the Glue job’s functionality, use cases, and configuration requirements.
