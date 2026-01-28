# Enhance Health Group ETL Pipeline

Healthcare data processing pipeline that extracts reports from CollaborateMD API, transforms them into CSV format, and loads them into PostgreSQL with analytical views for business intelligence and reporting.

## Project Structure

```
enhance_health_group/
├── config/
│   └── config.ini                # API credentials and database settings
├── sql/
│   └── psql-views.sql           # PostgreSQL analytical views
├── csv_files/                   # Generated CSV files (created during execution)
├── fetch_and_load_reports.py   # Main ETL script
└── generate_identifiers.py     # Report generation and tracking
```

## Architecture Overview

The pipeline operates in two distinct phases:

1. **Report Generation Phase**: Triggers report creation via CollaborateMD API
2. **ETL Phase**: Extracts, transforms, and loads the generated reports

## Detailed Component Analysis

### 1. Report Generation (`generate_identifiers.py`)

**Purpose**: Initiates report generation for multiple healthcare customer accounts

**Key Functions**:
- `generate_report_for_all_accounts()`: Iterates through customer accounts and triggers report generation
- `handle_report_response()`: Processes API responses and manages database state
- `postgres_connection()`: Establishes database connections

**Process Flow**:
1. Reads customer accounts from configuration
2. For each report type, sends POST request to CollaborateMD API
3. Parses XML response to extract report identifier and status
4. Updates PostgreSQL `account_reports` table with report metadata
5. Implements retry logic for reports still processing
6. Prevents duplicate report generation through database checks

**Database Schema**:
```sql
CREATE TABLE account_reports (
    customer_account VARCHAR,
    report_name VARCHAR,
    identifier VARCHAR,
    status INTEGER  -- 1 = active, 0 = inactive
);
```

**Error Handling**:
- Retry mechanism for reports still running (60-second intervals)
- Duplicate detection and prevention
- API failure handling with status code validation
- Database transaction management

### 2. Data Processing (`fetch_and_load_reports.py`)

**Purpose**: Extracts completed reports and loads them into PostgreSQL

**Key Functions**:
- `load_report_matrix()`: Retrieves active report identifiers from database
- `fetch_reports_to_csv()`: Downloads and processes report data
- `to_snake_case()`: Normalizes column names for database compatibility
- `load_csv_to_db()`: Creates database tables and applies views

**Extract Phase**:
1. Queries database for reports with status = 1 (completed)
2. Sends authenticated requests to CollaborateMD results endpoint
3. Receives base64-encoded ZIP files containing CSV data
4. Decodes and extracts CSV files from ZIP archives

**Transform Phase**:
1. **Column Normalization**: Converts headers to snake_case format
   - Removes special characters and spaces
   - Converts to lowercase with underscores
   - Handles edge cases (empty names → "unnamed_column")

2. **Data Standardization**:
   - Adds `customer_account` column to identify data source
   - Handles missing values and empty cells
   - Ensures consistent row length across all records

3. **File Consolidation**: Combines data from multiple customers into single CSV files per report type

**Load Phase**:
1. **Table Creation**: 
   - Drops existing tables with CASCADE to remove dependencies
   - Creates new tables using pandas `to_sql()` with schema specification
   - Handles data type inference automatically

2. **View Application**:
   - Sets PostgreSQL search_path to target schema
   - Executes analytical view definitions from `psql-views.sql`
   - Creates business intelligence layer on top of raw data

### 3. Analytical Views (`sql/psql-views.sql`)

**Purpose**: Provides business intelligence layer with calculated fields and aggregations

#### `ar_aging_view`
- **Function**: Accounts receivable aging analysis
- **Key Metrics**: Balance due by payer type, aging buckets, statement counts
- **Calculations**: 
  - Converts currency strings to numeric values
  - Creates aging categories (0-5 days, 6-10 days, etc.)
  - Calculates total statements sent per patient

#### `gross_billing_view`
- **Function**: Billing metrics and revenue analysis
- **Key Metrics**: Charge amounts, billing lag, revenue by service type
- **Calculations**:
  - Days on hold calculations (current date - charge entered date)
  - Billing lag (first billed date - charge entered date)
  - Date dimension fields (day, week, month, year)

#### `payment_trends_view`
- **Function**: Payment analysis and cash flow tracking
- **Key Metrics**: Payment amounts, posting turnaround time, payer performance
- **Calculations**:
  - Payment posting TAT (received date - entered date)
  - Insurance vs patient payment categorization
  - Applied vs unapplied payment amounts

#### `charges_on_hold`
- **Function**: Claims processing status monitoring
- **Key Metrics**: Total amounts by claim status and service type
- **Aggregations**: Grouped by facility, claim status, and CPT code

## Configuration

### Database Setup

1. **Create PostgreSQL Schema**:
```sql
CREATE SCHEMA enhance_health_group;
CREATE TABLE enhance_health_group.account_reports (
    customer_account VARCHAR(50),
    report_name VARCHAR(100),
    identifier VARCHAR(50),
    status INTEGER DEFAULT 0
);
```

2. **Configure Connection**: Edit `config/config.ini`:

```ini
[API]
report_api_base_url = https://webapi.collaboratemd.com/v1
username = your_api_username
password = your_api_password

[POSTGRES]
host = your_postgres_host
user = your_db_username
password = your_db_password
database = your_database_name
port = 5432
schema = enhance_health_group

[CUSTOMERS]
accounts = ['10028395', '10026936', '10026716', '10023994']
```

### Environment Variable Overrides

For production deployments, override sensitive settings:

```bash
export POSTGRES_HOST="production-host"
export POSTGRES_USER="prod_user"
export POSTGRES_PASSWORD="secure_password"
export API_USERNAME="api_user"
export API_PASSWORD="api_password"
```

## Usage

### Step-by-Step Execution

1. **Generate Reports** (Run first):
   ```bash
   cd enhance_health_group
   python generate_identifiers.py
   ```
   
   **What happens**:
   - Triggers report generation for all configured customers
   - Updates database with report identifiers
   - May take 30-60 minutes depending on report complexity
   - Check database for status updates

2. **Wait for Report Completion** (Check status):
   ```sql
   SELECT customer_account, report_name, identifier, status 
   FROM enhance_health_group.account_reports 
   WHERE status = 1;
   ```

3. **Run ETL Pipeline** (After reports complete):
   ```bash
   python fetch_and_load_reports.py
   ```
   
   **What happens**:
   - Downloads completed reports as ZIP files
   - Extracts and processes CSV data
   - Creates database tables (drops existing)
   - Applies analytical views
   - Generates files in `csv_files/` directory

### Automated Scheduling

**Daily ETL Process**:
```bash
#!/bin/bash
# Morning: Generate reports
python generate_identifiers.py

# Wait for completion (check every hour)
while [ $(psql -t -c "SELECT COUNT(*) FROM enhance_health_group.account_reports WHERE status = 1") -lt 12 ]; do
    sleep 3600
done

# Evening: Process completed reports
python fetch_and_load_reports.py
```

## Report Types and Business Value

| Report Type | Business Purpose | Key Metrics | Update Frequency |
|-------------|------------------|-------------|------------------|
| **AR Aging** | Track outstanding receivables | Balance by aging buckets, payer performance | Daily |
| **Charges on Hold** | Monitor claim processing delays | Claims by status, hold reasons | Daily |
| **Claim Stage Breakdown** | Analyze claim workflow | Processing stages, bottlenecks | Weekly |
| **Denial Trends** | Identify denial patterns | Denial rates by payer, reason codes | Weekly |
| **Gross Billing** | Revenue cycle performance | Billing volumes, charge lag | Daily |
| **Payment Trends** | Cash flow analysis | Payment posting, collection rates | Daily |
| **Quadrant Performance** | Provider productivity | Revenue by provider/location | Monthly |
| **RCM Productivity** | Operational efficiency | Staff productivity metrics | Weekly |
| **User Time Spread** | Resource allocation | Time tracking by user/task | Weekly |
| **Write-off Trends** | Revenue leakage analysis | Write-off amounts and reasons | Monthly |
| **PDR3 Calculator** | Regulatory compliance | Patient day calculations | Monthly |
| **Revenue Recognition** | Financial reporting | Charges vs payments reconciliation | Monthly |

### Report Configuration Details

```python
report_configs = [
    {"report_id": "10078378", "filter_id": "10141925", "name": "ar_aging"},
    {"report_id": "10078486", "filter_id": "10141929", "name": "gross_billing"},
    {"report_id": "10078375", "filter_id": "10141926", "name": "charges_on_hold"},
    # ... additional configurations in generate_identifiers.py
]
```

## Data Flow Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CollaborateMD │    │   Report Queue   │    │   ETL Pipeline  │
│       API       │───▶│   Management     │───▶│   Processing    │
│                 │    │                  │    │                 │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ XML Response    │    │ PostgreSQL       │    │ CSV Files       │
│ (Report Status) │    │ (Report Tracking)│    │ (Intermediate)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │ PostgreSQL      │
                                               │ (Data Tables +  │
                                               │ Analytical Views)│
                                               └─────────────────┘
```

### Technical Data Flow

1. **Report Initiation**: POST request with XML payload to trigger report generation
2. **Status Tracking**: XML response parsing and database state management
3. **Data Retrieval**: GET request returns base64-encoded ZIP containing CSV
4. **Data Processing**: ZIP extraction, CSV parsing, column normalization
5. **Database Loading**: Table creation, data insertion, view application

## Prerequisites and Dependencies

### Python Requirements
```bash
pip install requests==2.31.0
pip install psycopg2-binary==2.9.7
pip install pandas==2.0.3
pip install sqlalchemy==2.0.19
```

### System Requirements
- Python 3.8+
- PostgreSQL 12+
- Network access to CollaborateMD API
- Sufficient disk space for CSV file generation

### Database Permissions
```sql
GRANT CREATE, USAGE ON SCHEMA enhance_health_group TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA enhance_health_group TO etl_user;
GRANT CREATE ON DATABASE your_database TO etl_user;
```

## Monitoring and Troubleshooting

### Common Issues

1. **API Authentication Failures**
   ```
   Error: 401 Unauthorized
   Solution: Verify API credentials in config.ini
   ```

2. **Report Still Processing**
   ```
   Status: "Report still running"
   Solution: Wait 60 seconds, automatic retry implemented
   ```

3. **Database Connection Issues**
   ```
   Error: psycopg2.OperationalError
   Solution: Check PostgreSQL service, network connectivity, credentials
   ```

4. **CSV Processing Errors**
   ```
   Error: UnicodeDecodeError
   Solution: Check file encoding, verify ZIP file integrity
   ```

### Monitoring Queries

**Check Report Status**:
```sql
SELECT 
    customer_account,
    COUNT(*) as total_reports,
    SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) as completed_reports
FROM enhance_health_group.account_reports 
GROUP BY customer_account;
```

**View Recent Data Loads**:
```sql
SELECT 
    schemaname,
    tablename,
    n_tup_ins as rows_inserted,
    last_autoanalyze
FROM pg_stat_user_tables 
WHERE schemaname = 'enhance_health_group';
```

### Performance Optimization

- **Batch Processing**: Reports processed sequentially to avoid API rate limits
- **Memory Management**: Large CSV files processed in chunks using pandas
- **Database Optimization**: Tables dropped and recreated for clean data loads
- **Connection Pooling**: Database connections properly closed after use

## Security Considerations

- API credentials stored in configuration files (not version controlled)
- Database connections use SSL when available
- Environment variable overrides for production deployments
- Healthcare data handling follows HIPAA compliance standards

## Environment Variables

Override configuration settings:
- `POSTGRES_HOST` - Database server hostname
- `POSTGRES_USER` - Database username
- `POSTGRES_PASSWORD` - Database password
- `POSTGRES_DATABASE` - Target database name
- `API_USERNAME` - CollaborateMD API username
- `API_PASSWORD` - CollaborateMD API password