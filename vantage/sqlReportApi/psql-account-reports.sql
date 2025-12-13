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
('10031998', 'ar_aging', '12307827', 1),
('10031998', 'charges_on_hold', '12308027', 1),
('10031998', 'claim_stage_breakdown', '12308039', 1),
('10031998', 'denial_trends', '12308040', 1),
('10031998', 'gross_billing', '12308044', 1),
('10031998', 'payment_trend', '12308054', 1),
('10031998', 'quadrant_performance', '12308063', 1),
('10031998', 'rcm_productivity', '12308066', 1),
('10031998', 'user_time_spread', '12308071', 0),
('10031998', 'write_off_trend', '12308073', 1),

-- 10032172
('10032172', 'ar_aging', '12310297', 1),
('10032172', 'charges_on_hold', '12310303', 1),
('10032172', 'claim_stage_breakdown', '12310308', 1),
('10032172', 'denial_trends', '12310314', 1),
('10032172', 'gross_billing', '12310319', 1),
('10032172', 'payment_trend', '12310323', 1),
('10032172', 'quadrant_performance', '12310327', 1),
('10032172', 'rcm_productivity', '12310331', 1),
('10032172', 'user_time_spread', '12310402', 0),
('10032172', 'write_off_trend', '12310339', 1),

-- 10032272
('10032272', 'ar_aging', '12310300', 1),
('10032272', 'charges_on_hold', '12310304', 1),
('10032272', 'claim_stage_breakdown', '12310309', 1),
('10032272', 'denial_trends', '12310315', 1),
('10032272', 'gross_billing', '12310320', 1),
('10032272', 'payment_trend', '12310324', 1),
('10032272', 'quadrant_performance', '12310329', 1),
('10032272', 'rcm_productivity', '12310404', 1),
('10032272', 'user_time_spread', '12310411', 0),
('10032272', 'write_off_trend', '12310341', 1),

-- 10032271
('10032271', 'ar_aging', '12310302', 1),
('10032271', 'charges_on_hold', '12310305', 1),
('10032271', 'claim_stage_breakdown', '12310311', 1),
('10032271', 'denial_trends', '12310316', 1),
('10032271', 'gross_billing', '12310322', 1),
('10032271', 'payment_trend', '12310325', 1),
('10032271', 'quadrant_performance', '12310330', 1),
('10032271', 'rcm_productivity', '12310333', 1),
('10032271', 'user_time_spread', '12310337', 0),
('10032271', 'write_off_trend', '12310342', 1);
