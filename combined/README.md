# CollaborateMD ETL Pipeline

This ETL pipeline fetches report data from the CollaborateMD API, processes it, and loads it into a PostgreSQL database.

## API Execution Constraints

CollaborateMD enforces strict concurrency limits:
* **Single-Report Limit:** Only **one** report can be running at a time per instance (API credential set).
* **Sequential Triggering:** A new report cannot be triggered until the previous report has fully completed.

### Concurrency Strategy:
* **Sequential Execution (Per Instance):** Reports and accounts within a single instance are run one-by-one.
* **Parallel Execution (Cross-Instance):** Multiple instances (e.g., `enhance_health`, `vantage`) run concurrently in parallel threads using separate API credentials.

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
* **Schema-Driven Coercion:** If the database table already exists, the script reads the target table's schema and coerces CSV column types to match the database types (preventing type conflicts). If the table doesn't exist, it dynamically infers types.
* **Column Alignment:** Automatically reorders CSV columns to match the target database table's column order.
* **Database Load:** Truncates the target tables, imports the processed data, and runs SQL post-processing views (`sql/psql-views.sql`).

---

## Execution Flow

1. **Configure (`config/config.py`):** Set API credentials, PostgreSQL connection, customer accounts, and report mappings.
2. **Run `generate_identifiers.py`:** Initiates report generation and registers active identifiers in the database.
3. **Run `fetch_and_load_reports.py`:** Fetches the generated reports, transforms/coerces the data types, and loads them into PostgreSQL.
