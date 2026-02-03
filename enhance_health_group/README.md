
# Enhance Health Group ETL Pipeline

This document outlines the ETL (Extract, Transform, Load) pipeline for the Enhance Health Group project. The pipeline is responsible for fetching data from a third-party API, processing it, and loading it into a PostgreSQL database for further analysis.

## Pipeline Overview

The pipeline consists of three main stages:

1.  **Report Generation and Identifier Fetching**: This stage initiates the generation of various reports through a third-party API. Each report is assigned a unique identifier, which is used to track its status and retrieve the results.
2.  **Data Extraction and Transformation**: Once the reports are ready, this stage fetches the report data, which is in a compressed and encoded format. The data is then decoded, uncompressed, and transformed into a structured format (CSV).
3.  **Database Loading and Post-Processing**: The final stage loads the transformed data into a PostgreSQL database. After the data is loaded, a series of SQL views are created to provide a simplified and more analytical-friendly representation of the data.

## Pipeline Components

The pipeline is composed of the following components:

*   **Python Scripts**:
    *   `generate_identifiers.py`: This script is responsible for initiating the report generation process and fetching the unique identifiers for each report.
    *   `fetch_and_load_reports.py`: This script fetches the generated reports, processes the data, and loads it into the PostgreSQL database.
*   **Configuration File**:
    *   `config/config.ini`: This file contains all the necessary configurations for the pipeline, including API credentials, database connection details, and a list of customer accounts and reports to be processed.
*   **SQL Scripts**:
    *   `sql/psql-views.sql`: This script contains a series of SQL `CREATE OR REPLACE VIEW` statements that are executed after the data has been loaded into the database. These views provide a more structured and analytical-friendly representation of the data.

## Pipeline Steps in Detail

### 1. Report Generation and Identifier Fetching (`generate_identifiers.py`)

1.  **Configuration Loading**: The script begins by loading the necessary configurations from the `config/config.ini` file. This includes the API base URL, a list of customer accounts, and API credentials.
2.  **Report Iteration**: The script iterates through a predefined list of reports. For each report, it performs the following steps:
3.  **API Request**: An API call is made to initiate the generation of the report for each customer account.
4.  **Response Handling**: The API response is parsed to extract the report's status and a unique identifier.
5.  **Database Interaction**: The script connects to the PostgreSQL database and performs the following actions:
    *   It checks if the identifier already exists in the `account_reports` table to prevent duplicate entries.
    *   It updates the status of any previously active reports for the same customer and report type.
    *   It inserts a new record into the `account_reports` table with the new identifier and a status of "active".

### 2. Data Extraction and Transformation (`fetch_and_load_reports.py`)

1.  **Identifier Loading**: The script queries the `account_reports` table to get the list of active report identifiers for each customer account.
2.  **Data Fetching**: For each active report, the script makes an API call to fetch the report data. The data is returned in a compressed (ZIP) and base64-encoded format.
3.  **Data Decoding and Uncompression**: The script decodes the base64 string and uncompresses the ZIP archive to extract the CSV file containing the report data.
4.  **Data Transformation**: The script reads the CSV data, cleans it, and transforms it into a structured format. This includes:
    *   Converting column names to a consistent "snake\_case" format.
    *   Promoting columns to appropriate data types (numeric, date).
    *   Handling missing or empty values.
5.  **CSV File Generation**: The transformed data is then written to a new CSV file in the `csv_files` directory.

### 3. Database Loading and Post-Processing (`fetch_and_load_reports.py` and `sql/psql-views.sql`)

1.  **Database Loading**: The `load_csvs_to_db` function in `fetch_and_load_reports.py` reads the CSV files from the `csv_files` directory and loads them into the corresponding tables in the PostgreSQL database. The script performs the following steps:
    *   **Schema Validation**: It validates that the schema of the CSV file matches the schema of the corresponding table in the database.
    *   **Table Truncation**: Before loading the new data, the script truncates the target table to ensure a clean slate.
    *   **Data Loading**: The data from the CSV file is loaded into the table.
2.  **Post-Processing**: After the data has been loaded, the `run_sql_files` function is called. This function executes the SQL commands in the `sql/psql-views.sql` file. These commands create a series of views that provide a more user-friendly and analytical-friendly representation of the data. The views perform tasks such as:
    *   Joining tables.
    *   Calculating new fields.
    *   Renaming columns.
    *   Formatting data.

## How to Run the Pipeline

1.  **Configure the `config.ini` file**: Ensure that the API credentials, database connection details, and the list of customer accounts and reports are correctly configured in the `config/config.ini` file.
2.  **Run `generate_identifiers.py`**: Execute this script to start the report generation process and fetch the report identifiers.
3.  **Run `fetch_and_load_reports.py`**: After the reports have been generated, execute this script to fetch the data, transform it, and load it into the database.

