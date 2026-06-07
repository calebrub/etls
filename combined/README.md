# CollaborateMD ETL Pipeline

This ETL pipeline fetches report data from the CollaborateMD API, processes it, and loads it into a PostgreSQL database.

---

## 👔 Executive Summary (For Stakeholders & Leadership)

* **Robust & Self-Healing:** The pipeline automatically adapts to changes in CSV column ordering and dynamic type variations (e.g., nullable integer formats) without throwing errors or crashing, maintaining a reliable database state.
* **API Compliance & Protection:** Adheres strictly to CollaborateMD's execution constraints (sequential report execution per credential set) to prevent account lockout, IP throttling, or API bans.
* **Scalable Concurrency:** Scales horizontally across distinct medical billing entities. Multiple instances (e.g., `enhance_health`, `vantage`) are processed concurrently in parallel threads, keeping pipeline execution times low.

---

## 🛠️ Developer & Contributor Guide

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

## Core Scripts

### 1. `generate_identifiers.py`
Initiates report runs on CollaborateMD and records execution state.
* Loops through reports and accounts sequentially per instance.
* Triggers each report, polls the API until completion, and retrieves a unique `identifier`.
* Saves the active identifier to the database `account_reports` table (deactivating old runs).
* Parallelized across instances using Python's `ThreadPoolExecutor`.

### 2. `fetch_and_load_reports.py`
Downloads completed report data, processes it, and imports it to PostgreSQL.
* **Parallel Fetch:** Queries active identifiers and downloads base64-encoded ZIP files from the API concurrently (up to 32 threads).
* **Schema-Driven Coercion:** Coerces CSV column types to match existing database types (preventing type conflicts). Falls back to dynamic inference for new tables.
* **Column Alignment:** Automatically reorders CSV columns to match the target database table's column order.
* **Database Load:** Truncates the target tables, imports the processed data, and runs SQL post-processing views (`sql/psql-views.sql`).

---

## Execution Flow

1. **Configure (`config/config.py`):** Set API credentials, PostgreSQL connection, customer accounts, and report mappings.
2. **Run `generate_identifiers.py`:** Initiates report generation and registers active identifiers in the database.
3. **Run `fetch_and_load_reports.py`:** Fetches the generated reports, transforms/coerces the data types, and loads them into PostgreSQL.

---

## 🤖 n8n Workflow Automation

An automated n8n workflow coordinates the ETL execution schedule and triggers the core scripts sequentially.

* **Workflow Host:** [http://20.15.226.138/](http://20.15.226.138/)
* **Workflow JSON Definition:** [Reporting SAS ETL.json](file:///Users/caleb/IdeaProjects/etls/combined/Reporting%20SAS%20ETL.json)

### Automation Steps:
1. **Trigger:** Daily at 3:00 AM (via Schedule Trigger) or via Manual Trigger.
2. **Run Generate Identifiers Script:** Executes `generate_identifiers.py` to initiate report runs on the CollaborateMD API and register identifiers.
3. **Read & Parse Identifier Summary:** Reads `latest_identifier_summary.csv` and parses the CSV into a table.
4. **Wait 2 Minutes:** Pauses execution to allow time for the API to completely process the reports.
5. **Run Fetch and Load Reports Script:** Executes `fetch_and_load_reports.py` to download, coerce, and load the reports into PostgreSQL.
6. **Read & Parse Fetch Summary:** Reads `latest_fetch_summary.csv` and parses the CSV into a table.
7. **Join Run and Fetch Summaries:** Joins the generation run and fetch run summaries on `instance_key`, `customer_account`, and `report_name` for audit visibility.
8. **Convert Joined Table to CSV:** Saves the joined table back into a CSV file for unified monitoring.

