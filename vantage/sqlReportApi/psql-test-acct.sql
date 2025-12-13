-- Step 1: Create the table if not exists
CREATE TABLE IF NOT EXISTS account_reports (
    id SERIAL PRIMARY KEY,
    customer_account VARCHAR(20) NOT NULL,
    report_name VARCHAR(100) NOT NULL,
    identifier VARCHAR(20) NOT NULL,
    status INTEGER DEFAULT 1
);

-- Step 2: Create index for performance (optional)
CREATE INDEX IF NOT EXISTS idx_account_report ON account_reports (customer_account, report_name);

-- Step 3: Insert full dataset

INSERT INTO account_reports (customer_account, report_name, identifier, status) VALUES
-- 10031998
('10031998', 'ar_aging', '12340615', 1);