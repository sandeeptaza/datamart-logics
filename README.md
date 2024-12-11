# AWS Glue Job for Incremental and Full Data Loads with Table Joins

This project demonstrates an AWS Glue job that performs incremental and full data loads from a source database, joins multiple tables, and writes the output as Parquet files to Amazon S3. The solution dynamically determines whether to perform a full or incremental load based on the presence of a last load timestamp stored in S3.

---

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [How It Works](#how-it-works)
- [Job Parameters](#job-parameters)
- [Folder Structure](#folder-structure)
- [Usage](#usage)
- [Error Handling](#error-handling)
- [Logging](#logging)
- [License](#license)

---

## Overview

The script:
- Reads from multiple source tables.
- Performs a full load if no last load timestamp is found or an incremental load otherwise.
- Joins data from multiple tables based on predefined relationships.
- Writes the final dataset as Parquet files to S3.
- Updates the last load timestamp in S3 after each successful run.

---

## Features

- **Incremental Loading**: Only loads data modified since the last run.
- **Full Loading**: Supports initial full table loads.
- **Dynamic Joins**: Combines multiple tables based on predefined relationships.
- **Timestamp Management**: Retrieves and updates the last load timestamp in S3.
- **Error Handling**: Graceful error handling with logging for debugging.

---

## Prerequisites

1. AWS Glue environment set up.
2. An S3 bucket for:
   - Storing the last load timestamp (e.g., `s3://<bucket-name>/path/to/timestamp.csv`).
   - Saving the Parquet output.
3. Source database crawled and cataloged in AWS Glue.
4. IAM Role for the Glue job with permissions for Glue, S3, and logging.

---

## How It Works

1. **Load Type Determination**:
   - The script checks if the `last_load_timestamp` file exists in S3.
   - If the file is missing or the `load_type` is explicitly set to `full`, a full load is performed.
   - Otherwise, an incremental load is executed.

2. **Data Loading**:
   - For incremental loads, rows with timestamps greater than `last_load_timestamp` are filtered.
   - For full loads, all rows are processed.

3. **Data Transformation**:
   - Joins are performed between tables based on predefined keys.

4. **Data Writing**:
   - The final joined dataset is saved as Parquet files in the specified S3 location.

5. **Timestamp Update**:
   - The script updates the `last_load_timestamp` file in S3 with the latest timestamp from the processed data.

---

## Job Parameters


- **`database_name`**: The Glue catalog database containing the source tables.
- **`timestamp_s3_path`**: The S3 path where the `last_load_timestamp` is stored (e.g., `s3://<bucket-name>/path/to/timestamp.csv`).
- **`load_type`**: Specifies whether to force a full load (`full`) or let the script decide based on `timestamp_s3_path`.

---


---

## Usage

1. **Prepare the Environment**:
   - Ensure all source tables are crawled and available in the Glue catalog.
   - Upload an initial `timestamp.csv` file (if available) to the specified S3 location.



2. **Monitor Logs**:
   - View job execution logs in Amazon CloudWatch.

---

## Error Handling

- **Missing Tables or Columns**: Logs a warning and skips the table if expected columns are missing.
- **Join Failures**: Logs an error and exits if the base table or critical joins fail.
- **S3 File Errors**: Handles `NoSuchKey` gracefully when the timestamp file is missing.

---

## Logging

The script uses Python's `logging` module. Logs are available in CloudWatch under the Glue job's log group. Key logging features:
- **INFO**: Tracks major steps and table processing.
- **WARNING**: Highlights missing data or skipped steps.
- **ERROR**: Captures critical failures.

---

