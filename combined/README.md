# CollaborateMD ETL Pipeline

This ETL pipeline fetches report data from the CollaborateMD API, processes it, and loads it into a PostgreSQL database.

---

## Executive Summary (For Stakeholders & Leadership)

* **Robust & Self-Healing:** The pipeline automatically adapts to changes in CSV column ordering and dynamic type variations (e.g., nullable integer formats) without throwing errors or crashing, maintaining a reliable database state.
* **API Compliance & Protection:** Adheres strictly to CollaborateMD's execution constraints (sequential report execution per credential set) to prevent account lockout, IP throttling, or API bans.
* **Scalable Concurrency:** Scales horizontally across distinct medical billing entities. Multiple instances (e.g., `enhance_health`, `vantage`) are processed concurrently in parallel threads, keeping pipeline execution times low.

---

## Developer & Contributor Guide

* **Database Schema as Source of Truth:** Rather than guessing types from dynamic CSV data, the ETL queries PostgreSQL types (`text`, `bigint`, `date`, etc.) to automatically coerce CSV column data types before importing.
* **Adding New Reports:** When a new report is run for the first time:
  1. The ETL falls back to dynamic type inference.
  2. Creates the new table dynamically.
  3. Uses schema-driven coercion on all subsequent runs.
* **Schema Evolution:** If you change a database column type (e.g., from `bigint` to `text`), the ETL will automatically conform the incoming CSV data to the new type. If you introduce new columns, you must add them to PostgreSQL first, or the validation step will halt.
* **Audit & Monitoring Logs:** Execution metadata is written to `csv_files/run_summaries/` and `csv_files/fetch_summaries/`. These logs track row counts, API latency, retries, and errors for quick troubleshooting.

---

## API Execution Constraints

CollaborateMD enforces strict concurrency limits:
* **Single-Report Limit:** Only **one** report can be running at a time per instance (API credential set).
* **Sequential Triggering:** A new report cannot be triggered until the previous report has fully completed.

### Concurrency Strategy:
* **Sequential Execution (Per Instance):** Reports and accounts within a single instance are run one-by-one.
* **Parallel Execution (Cross-Instance):** Multiple instances run concurrently in parallel threads using separate API credentials.

---

## Core Scripts & Mechanics

This section describes how the pipeline works under the hood, detailing the roles of the two main scripts.

### 1. Report Generation: `generate_identifiers.py`
This script initiates the report generation process on the CollaborateMD SOAP/REST XML API and updates the execution state in PostgreSQL.

* **Sequential Execution (Per Credentials):** To adhere to CollaborateMD's strict execution constraints, the script loops through each customer account and requested report sequentially per credential set.
* **Cross-Instance Parallelism:** If multiple entity configurations (instances) exist, the script runs them concurrently in separate threads using Python's `ThreadPoolExecutor`. This maximizes throughput without crossing the API's concurrent execution limit.
* **API Polling & Backoff:** When a report request is sent, the API may return a status indicating the report is `"still running"`. The script automatically enters a polling loop, waiting 60 seconds before retrying.
* **Database Tracking (`account_reports`):** Once a report runs successfully, the script extracts the unique `identifier` from the XML response. It marks any existing run for that account/report as inactive (`status = 0`) and records the new active run (`status = 1`).
* **Audit Summaries:** After finishing all runs, execution statistics (HTTP response codes, API statuses, identifiers, and retry counts) are gathered and written to a timestamped CSV summary under `csv_files/run_summaries/latest_identifier_summary.csv`.

### 2. Fetch & Load: `fetch_and_load_reports.py`
This script retrieves the completed report data, cleans and validates the CSVs, coerces data types to match database tables, and imports the records into PostgreSQL.

* **Concurrent Downloads:** The script queries the database for all active (`status = 1`) report identifiers and downloads base64-encoded ZIP payloads concurrently (up to 32 parallel threads).
* **Extraction & Metadata Enrichment:** It base64-decodes the zip data, extracts the underlying CSV, cleans column headers into snake_case, and appends entity identifier columns (`customer_account` and `instance_key`) to each row.
* **Strict Schema Coercion:** Instead of relying on generic type inferences that can break on empty values or varying CSV schemas, the script queries the target PostgreSQL table's structure (`information_schema.columns`). It automatically coerces CSV strings into matching database types (such as mapping date fields, handling floating decimals, or using modern nullable integer `Int64` formats).
* **Schema Evolution & Validation:** 
  * If a report runs for the first time, it infers the schema dynamically and creates the table.
  * If the table already exists, it performs a pre-flight schema alignment and validation. Columns are auto-reordered to match the database order. If there's an irreconcilable structure mismatch (e.g., different column count or type differences), it halts before writing to protect the database integrity.
* **Database Update & Views Execution:** It safely truncates target tables and performs bulk uploads using SQLAlchemy/pandas. Finally, it executes all database post-processing SQL files in the `sql/` folder (such as refreshing downstream reporting views).
* **Audit Summaries:** Saves metadata details for all loaded tables in `csv_files/fetch_summaries/latest_fetch_summary.csv`.

---

## Execution Flow

1. **Configure (`config/config.py`):** Set API credentials, PostgreSQL connection, customer accounts, and report mappings.
2. **Run `generate_identifiers.py`:** Initiates report generation, polls the API for completion, and registers active identifiers in the database.
3. **Run `fetch_and_load_reports.py`:** Fetches the generated reports in parallel, cleans/coerces the data types, validates schemas, and loads them into PostgreSQL, followed by post-processing views.

---

## n8n Workflow Automation

An automated n8n workflow coordinates the ETL execution schedule and triggers the core scripts sequentially.

* **Workflow Host:** [http://20.15.226.138/](http://20.15.226.138/)
* **Workflow JSON Definition:** [Reporting SAS ETL.json](file:///Users/caleb/IdeaProjects/etls/combined/Reporting%20SAS%20ETL.json)

### Automation Steps:
1. **Trigger:** Daily at 3:00 AM Eastern Time (ET / America/New_York) (via Schedule Trigger) or via Manual Trigger.
2. **Run Generate Identifiers Script:** Executes `generate_identifiers.py` to initiate report runs on the CollaborateMD API and register identifiers.
3. **Read & Parse Identifier Summary:** Reads `latest_identifier_summary.csv` and parses the CSV into a table.
4. **Wait 2 Minutes:** Pauses execution to allow time for the API to completely process the reports.
5. **Run Fetch and Load Reports Script:** Executes `fetch_and_load_reports.py` to download, coerce, and load the reports into PostgreSQL.
6. **Read & Parse Fetch Summary:** Reads `latest_fetch_summary.csv` and parses the CSV into a table.
7. **Join Run and Fetch Summaries:** Joins the generation run and fetch run summaries on `instance_key`, `customer_account`, and `report_name` for audit visibility.
8. **Convert Joined Table to CSV:** Saves the joined table back into a CSV file for unified monitoring.

