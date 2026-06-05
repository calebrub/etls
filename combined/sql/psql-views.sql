DROP MATERIALIZED VIEW IF EXISTS gross_billing_view;
CREATE MATERIALIZED VIEW gross_billing_view AS
WITH gross_billing_cte AS (
    SELECT
        gb.customer_account::varchar AS customer_account,
        gb.practice_name AS office_name,
        gb.practice_name AS facility_name,
        gb.practice_name AS practice_name,
        gb.charge_primary_payer_name,
        gb.charge_patient_id::varchar AS charge_patient_id,
        gb.charge_id::varchar AS charge_id,
        gb.charge_claim_id::varchar AS charge_claim_id,
        gb.patient_full_name,
        gb.primary_payer_member_id,
        gb.charge_from_date,
        gb.charge_to_date,
        gb.charge_entered_date,
        gb.type_of_bill::varchar AS type_of_bill,
        gb.claim_first_billed_date,
        gb.charge_cpt_code,
        gb.charge_units_sum::varchar AS charge_units_sum,
        gb.charge_amount,
        gb.charge_rev_code,
        gb.charge_current_payer_name,
        gb.claim_status,
        gb.charge_entered_date::date AS date_charge_entered_date,
        (NOW() AT TIME ZONE 'MST')::date - gb.charge_entered_date::date AS days_on_hold,
        gb.claim_first_billed_date::date - gb.charge_entered_date::date AS charge_lag,
        CASE
            WHEN gb.claim_first_billed_date IS NOT NULL THEN 'Billed'
            ELSE 'Not Billed'
        END AS billed_or_not_billed,
        to_char(gb.claim_first_billed_date::date::timestamp with time zone, 'day') AS claim_first_billed_day,
        'Week' || to_char(gb.claim_first_billed_date::date::timestamp with time zone, 'IW') AS claim_first_billed_week,
        to_char(gb.claim_first_billed_date::date::timestamp with time zone, 'Month') AS claim_first_billed_month,
        to_char(gb.claim_first_billed_date::date::timestamp with time zone, 'YYYY') AS claim_first_billed_year,
        -- cleaned codes for LOC lookup
        ltrim(split_part(regexp_replace(gb.charge_rev_code, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(gb.charge_cpt_code, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        replace(replace(gb.charge_amount::text, '$', ''), ',', '')::numeric AS int_charge_amount,
        gb.claim_first_billed_date::date - EXTRACT(dow FROM gb.claim_first_billed_date::date)::integer AS first_billed_week_date,
        gb.instance_key,
        CASE
            WHEN gb.charge_primary_payer_name ILIKE 'self pay' THEN gb.charge_entered_date
            ELSE gb.claim_first_billed_date
        END AS claim_first_billed_date_cln,
        CASE
            WHEN gb.claim_first_billed_date IS NULL AND gb.charge_primary_payer_name NOT ILIKE 'self pay' THEN 0
            ELSE replace(replace(gb.charge_amount::text, '$', ''), ',', '')::numeric
        END AS int_charge_amount_cln
    FROM gross_billing gb
)
SELECT
    customer_account,
    office_name,
    facility_name,
    practice_name,
    charge_primary_payer_name,
    charge_patient_id,
    charge_id,
    charge_claim_id,
    patient_full_name,
    primary_payer_member_id,
    charge_from_date,
    charge_to_date,
    charge_entered_date,
    type_of_bill,
    claim_first_billed_date,
    charge_cpt_code,
    charge_units_sum,
    charge_amount,
    charge_rev_code,
    charge_current_payer_name,
    claim_status,
    date_charge_entered_date,
    days_on_hold,
    charge_lag,
    billed_or_not_billed,
    claim_first_billed_day,
    claim_first_billed_week,
    claim_first_billed_month,
    claim_first_billed_year,
    coalesce(loc1.level_of_care, loc2.level_of_care) AS loc,
    int_charge_amount,
    first_billed_week_date,
    CASE
        WHEN days_on_hold <= 5 THEN '0-5 days'
        WHEN days_on_hold > 5 AND days_on_hold <= 10 THEN '6-10 days'
        WHEN days_on_hold > 10 AND days_on_hold < 15 THEN '11-14 days'
        WHEN days_on_hold >= 15 AND days_on_hold <= 21 THEN '15-21 days'
        WHEN days_on_hold > 21 AND days_on_hold <= 30 THEN '22-30 days'
        WHEN days_on_hold > 30 AND days_on_hold <= 45 THEN '31-45 days'
        WHEN days_on_hold > 45 AND days_on_hold <= 60 THEN '46-60 days'
        WHEN days_on_hold > 60 AND days_on_hold <= 90 THEN '61-90 days'
        WHEN days_on_hold > 90 THEN 'over 90 days'
        ELSE NULL
    END AS days_on_hold_range,
    concat(claim_first_billed_month, '', claim_first_billed_year) AS claim_first_billed_ym,
    instance_key,
    claim_first_billed_date_cln,
    int_charge_amount_cln,
    pnc.payer_code AS payer_class
FROM gross_billing_cte
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = gross_billing_cte.charge_primary_payer_name;

CREATE INDEX IF NOT EXISTS idx_gross_billing_account ON gross_billing_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_gross_billing_entered_date ON gross_billing_view(charge_entered_date);
CREATE INDEX IF NOT EXISTS idx_gross_billing_billed_date ON gross_billing_view(claim_first_billed_date);
CREATE INDEX IF NOT EXISTS idx_gross_billing_instance_key ON gross_billing_view(instance_key);
----

----

DROP MATERIALIZED VIEW IF EXISTS payment_trends_view;
CREATE MATERIALIZED VIEW payment_trends_view
            (customer_account, facility_name, office_name, practice_name, charge_entered_date, charge_from_date,
             charge_to_date, patient_full_name, payment_source, payment_allowed_amount, charge_patient_id, charge_id,
             charge_rev_code, charge_cpt_code, type_of_bill, charge_claim_id, payer_name, primary_payer_member_id,
             charge_amount, insurance_paid_amount, payment_total_paid, payment_total_applied,
             charge_insurance_adjustments, charge_patient_adjustments, charge_total_adjustments, payment_received,
             payment_entered, insurance_applied_amount, patient_applied_amount, payment_applied_amount,
             payment_unapplied_amount, charge_from_day, charge_from_week, charge_from_month, charge_from_year,
             payment_status, int_payment_allowed_amount, level_of_care, has_insurance_payment,
             int_insurance_paid_amount, payment_received_day, payment_received_week, payment_received_month,
             payment_received_year, payment_posting_tat, int_payment_applied_amount, int_payment_total_applied,
             int_payment_total_paid, int_payment_unapplied_amount, payer_class, instance_key)
AS
SELECT
    pt.customer_account,
    pt.practice_name AS facility_name,
    pt.practice_name AS office_name,
    pt.practice_name,
    pt.charge_entered_date,
    pt.charge_from_date,
    pt.charge_to_date,
    pt.patient_full_name,
    'Patient' AS payment_source,
    pt.payment_allowed_amount,
    pt.charge_patient_id,
    pt.charge_id AS charge_id,
    pt.charge_rev_code AS charge_rev_code,
    pt.charge_cpt_code,
    pt.type_of_bill,
    pt.claim_id AS charge_claim_id,
    pt.charge_primary_payer_name AS payer_name,
    pt.primary_payer_member_id,
    pt.charge_amount,
    replace(replace(pt.insurance_paid_amount::text, '$', ''), ',', '')::numeric AS insurance_paid_amount,
    pt.payment_total_paid,
    pt.payment_total_applied,
    pt.charge_insurance_adjustments,
    pt.charge_patient_adjustments,
    pt.charge_total_adjustments,
    pt.payment_received,
    pt.payment_entered,
    pt.insurance_applied_amount,
    pt.patient_applied_amount,
    pt.payment_total_applied AS payment_applied_amount,
    pt.payment_unapplied_amount,
    initcap(to_char(pt.charge_from_date::date::timestamp with time zone, 'day')) AS charge_from_day,
    'Week' || to_char(pt.charge_from_date::date::timestamp with time zone, 'IW') AS charge_from_week,
    to_char(pt.charge_from_date::date::timestamp with time zone, 'Month') AS charge_from_month,
    to_char(pt.charge_from_date::date::timestamp with time zone, 'YYYY') AS charge_from_year,
    CASE
        WHEN replace(replace(pt.payment_allowed_amount::text, '$', ''), ',', '')::numeric > 0 THEN 'Paid'
        ELSE 'Not Paid'
    END AS payment_status,
    replace(replace(pt.payment_allowed_amount::text, '$', ''), ',', '')::numeric AS int_payment_allowed_amount,
    coalesce(loc1.level_of_care, loc2.level_of_care) AS level_of_care,
    CASE
        WHEN replace(replace(pt.insurance_paid_amount::text, '$', ''), ',', '')::numeric > 0 THEN true
        ELSE false
    END AS has_insurance_payment,
    replace(replace(pt.insurance_paid_amount::text, '$', ''), ',', '')::numeric AS int_insurance_paid_amount,
    initcap(to_char(pt.payment_received::date::timestamp with time zone, 'day')) AS payment_received_day,
    'Week' || to_char(pt.payment_received::date::timestamp with time zone, 'IW') AS payment_received_week,
    to_char(pt.payment_received::date::timestamp with time zone, 'Month') AS payment_received_month,
    to_char(pt.payment_received::date::timestamp with time zone, 'YYYY') AS payment_received_year,
    pt.payment_received::date - pt.payment_entered::date AS payment_posting_tat,
    replace(replace(pt.patient_applied_amount::text, '$', ''), ',', '')::numeric AS int_payment_applied_amount,
    replace(replace(pt.payment_total_applied::text, '$', ''), ',', '')::numeric AS int_payment_total_applied,
    replace(replace(pt.payment_total_paid::text, '$', ''), ',', '')::numeric AS int_payment_total_paid,
    replace(replace(pt.payment_unapplied_amount::text, '$', ''), ',', '')::numeric AS int_payment_unapplied_amount,
    pnc.payer_code AS payer_class,
    instance_key
FROM payment_trend pt
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = ltrim(split_part(regexp_replace(pt.charge_rev_code, '\.0$', ''), ' ', 1), '0')
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = ltrim(split_part(regexp_replace(pt.charge_cpt_code, '\.0$', ''), ' ', 1), '0')
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = pt.charge_primary_payer_name;

CREATE INDEX IF NOT EXISTS idx_payment_trends_account ON payment_trends_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_payment_trends_received_date ON payment_trends_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_payment_trends_entered_date ON payment_trends_view(charge_entered_date);
CREATE INDEX IF NOT EXISTS idx_payment_trends_instance_key ON payment_trends_view(instance_key);

----

DROP MATERIALIZED VIEW IF EXISTS charges_on_hold_view;
CREATE MATERIALIZED VIEW charges_on_hold_view AS
WITH charges_on_hold_cte AS (
    SELECT
        coh.customer_account::varchar AS customer_account,
        coh.instance_key,
        coh.practice_name,
        coh.facility_name,
        coh.office_name,
        coh.patient_id::varchar AS patient_id,
        coh.charge_id::varchar AS charge_id,
        coh.claim_id::varchar AS claim_id,
        coh.patient_full_name,
        coh.charge_entered_date,
        coh.charge_cpt_code,
        coh.charge_rev_code,
        coh.type_of_bill::varchar AS type_of_bill,
        coh.charge_entered_age_days,
        coh.charge_from_date,
        coh.charge_to_date,
        coh.charge_amount,
        coh.claim_status,
        coh.charge_set_to_status,
        coh.created_at,
        -- cleaned codes for LOC lookup
        ltrim(split_part(regexp_replace(coh.charge_rev_code, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(coh.charge_cpt_code, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        -- derived fields
        coh.charge_entered_date::date AS date_charge_entered,
        (NOW() AT TIME ZONE 'MST')::date - coh.charge_entered_date::date AS days_on_hold,
        replace(replace(coh.charge_amount::text, '$', ''), ',', '')::numeric AS int_charge_amount,
        coh.charge_primary_payer_name
    FROM charges_on_hold coh
)
SELECT
    customer_account,
    instance_key,
    practice_name,
    facility_name,
    office_name,
    patient_id,
    charge_id,
    claim_id,
    patient_full_name,
    charge_entered_date,
    charge_cpt_code,
    charge_rev_code,
    coalesce(loc1.level_of_care, loc2.level_of_care) AS loc,
    type_of_bill,
    charge_entered_age_days,
    charge_from_date,
    charge_to_date,
    charge_amount,
    claim_status,
    charge_set_to_status,
    created_at,
    date_charge_entered,
    days_on_hold,
    CASE
        WHEN days_on_hold <= 5 THEN '0-5 days'
        WHEN days_on_hold > 5 AND days_on_hold <= 10 THEN '6-10 days'
        WHEN days_on_hold > 10 AND days_on_hold < 15 THEN '11-14 days'
        WHEN days_on_hold >= 15 AND days_on_hold <= 21 THEN '15-21 days'
        WHEN days_on_hold > 21 AND days_on_hold <= 30 THEN '22-30 days'
        WHEN days_on_hold > 30 AND days_on_hold <= 45 THEN '31-45 days'
        WHEN days_on_hold > 45 AND days_on_hold <= 60 THEN '46-60 days'
        WHEN days_on_hold > 60 AND days_on_hold <= 90 THEN '61-90 days'
        WHEN days_on_hold > 90 THEN 'over 90 days'
        ELSE NULL
    END AS days_on_hold_range,
    int_charge_amount,
    pnc.payer_code as payer_class,
    charge_primary_payer_name
FROM charges_on_hold_cte
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = charge_primary_payer_name;

CREATE INDEX IF NOT EXISTS idx_charges_on_hold_account ON charges_on_hold_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_charges_on_hold_date ON charges_on_hold_view(charge_entered_date);
CREATE INDEX IF NOT EXISTS idx_charges_on_hold_instance_key ON charges_on_hold_view(instance_key);

-- Cluster the data physically by entered date to speed up chronological extracts
CLUSTER gross_billing_view USING idx_gross_billing_entered_date;

----

DROP MATERIALIZED VIEW IF EXISTS ar_aging_view;
CREATE MATERIALIZED VIEW ar_aging_view AS
WITH ar_aging_cte AS (
    SELECT
        aa.customer_account,
        aa.charge_id,
        aa.claim_id,
        aa.claim_first_billed_date,
        aa.charge_from_date,
        aa.charge_to_date,
        aa.charge_entered_date,
        aa.charge_primary_payer_name,
        aa.charge_primary_payer_id,
        aa.practice_name,
        aa.practice_name AS office_name,
        aa.charge_primary_payer_name AS charge_current_payer_name,
        aa.practice_name AS facility_name,
        regexp_replace(aa.patient_full_name, '\s*\(\d+\)$', '') AS patient_full_name,
        aa.patient_id AS claim_patient_id,
        aa.charge_cpt_code,
        aa.charge_cpt_description,
        aa.revenue_code,
        aa.charge_fromdate_age,
        aa.charge_fromdate_age_days,
        aa.charge_first_bill_date_age,
        aa.charge_first_bill_date_age_days,
        aa.charge_balance,
        aa.charge_balance_due_ins,
        aa.charge_balance_due_other,
        aa.charge_balance_due_pat,
        aa.charge_balance_at_collections,
        aa.charge_insurance_payments,
        aa.charge_patient_payments,
        aa.charge_total_payments,
        aa.patient_stmts_sent_electronically,
        aa.patient_statements_printed,
        aa.instance_key,

        -- LOC
        lc.level_of_care AS loc,

        -- Numeric conversions
        replace(replace(aa.charge_balance_due_ins::text, '$', ''), ',', '')::numeric AS int_charge_balance_due_ins,
        replace(replace(aa.charge_balance_due_other::text, '$', ''), ',', '')::numeric AS int_charge_balance_due_other,
        replace(replace(aa.charge_balance_due_pat::text, '$', ''), ',', '')::numeric AS int_charge_balance_due_pat,
        aa.patient_stmts_sent_electronically::numeric AS int_patient_stmts_sent_electronically,
        aa.patient_statements_printed::numeric AS int_patient_statements_printed,

        -- Lifecycle calculations
        (NOW() AT TIME ZONE 'MST')::date - aa.charge_entered_date::date AS days_on_hold,
        aa.claim_first_billed_date::date - aa.charge_entered_date::date AS charge_lag,

        CASE
            WHEN aa.claim_first_billed_date IS NOT NULL THEN 'Billed'
            ELSE 'Not Billed'
            END AS billed_or_not_billed,

        to_char(aa.claim_first_billed_date::date::timestamp with time zone, 'day')
            AS claim_first_billed_day,
        'Week' || to_char(aa.claim_first_billed_date::date::timestamp with time zone, 'IW')
            AS claim_first_billed_week,
        to_char(aa.claim_first_billed_date::date::timestamp with time zone, 'Month')
            AS claim_first_billed_month,
        to_char(aa.claim_first_billed_date::date::timestamp with time zone, 'YYYY')
            AS claim_first_billed_year,

        aa.claim_first_billed_date::date
            - EXTRACT(dow FROM aa.claim_first_billed_date::date)::integer
            AS first_billed_week_date,

        -- CLN logic
        CASE
            WHEN aa.charge_primary_payer_name ILIKE 'self pay'
                THEN aa.charge_entered_date
            ELSE aa.claim_first_billed_date
            END AS claim_first_billed_date_cln

    FROM ar_aging aa
             LEFT JOIN loc_crosswalk lc
                       ON lc.rev_code::text = aa.revenue_code::text
)

SELECT
    ac.*,

    -- Aggregates
    ac.int_charge_balance_due_ins + ac.int_charge_balance_due_other AS balance_due_payer,
    ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically AS total_statements,

    CASE
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) < 3
            THEN 'less than 3 statements'
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) BETWEEN 3 AND 4
            THEN '3-4 statements'
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) BETWEEN 5 AND 6
            THEN '5-6 statements'
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) > 6
            THEN 'over 6 statements'
        ELSE NULL
        END AS total_statement_buckets,

    CASE
        WHEN days_on_hold <= 5 THEN '0-5 days'
        WHEN days_on_hold > 5 AND days_on_hold <= 10 THEN '6-10 days'
        WHEN days_on_hold > 10 AND days_on_hold < 15 THEN '11-14 days'
        WHEN days_on_hold >= 15 AND days_on_hold <= 21 THEN '15-21 days'
        WHEN days_on_hold > 21 AND days_on_hold <= 30 THEN '22-30 days'
        WHEN days_on_hold > 30 AND days_on_hold <= 45 THEN '31-45 days'
        WHEN days_on_hold > 45 AND days_on_hold <= 60 THEN '46-60 days'
        WHEN days_on_hold > 60 AND days_on_hold <= 90 THEN '61-90 days'
        WHEN days_on_hold > 90 THEN 'over 90 days'
        ELSE NULL
        END AS days_on_hold_range,

    concat(ac.claim_first_billed_month, '', ac.claim_first_billed_year)
        AS claim_first_billed_ym,

    pc.payer_code as payer_class

FROM ar_aging_cte ac
         LEFT JOIN payer_name_crosswalk pc
                   ON pc.payer_name::text = ac.charge_primary_payer_name;

CREATE INDEX IF NOT EXISTS idx_ar_aging_account ON ar_aging_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_ar_aging_instance_key ON ar_aging_view(instance_key);

------------------------
DROP MATERIALIZED VIEW IF EXISTS pdr3_calculator_view CASCADE;
CREATE MATERIALIZED VIEW pdr3_calculator_view AS
WITH base AS (
    SELECT
        p.*,

        -- 1. Location Code
        split_part(p.practice_name, ' ', 1) AS location_code,

        -- 2. Clean numeric fields
        replace(replace(p.insurance_paid_amount, '$', ''), ',', '')::numeric AS int_insurance_paid_amount,
        replace(replace(p.patient_paid_amount_w_copays, '$', ''), ',', '')::numeric AS int_patient_paid_amount,
        replace(replace(p.charge_balance_due_ins, '$', ''), ',', '')::numeric AS int_charge_balance_due_ins,
        replace(replace(p.charge_balance_due_pat, '$', ''), ',', '')::numeric AS int_charge_balance_due_pat,
        replace(replace(p.charge_balance_at_collections, '$', ''), ',', '')::numeric AS int_charge_balance_at_collections,

        -- 3. Total Payment Received
        (
            COALESCE(replace(replace(p.insurance_paid_amount, '$', ''), ',', '')::numeric, 0) +
            COALESCE(replace(replace(p.patient_paid_amount_w_copays, '$', ''), ',', '')::numeric, 0)
        ) AS total_payment_received

    FROM pdr3_calculator p

    --
    WHERE
        -- last 12 months based on payment_entered
--        p.payment_entered >= (NOW() AT TIME ZONE 'MST')::date - INTERVAL '12 months'
--         p.payment_entered >= '2026-03-08'::date - INTERVAL '12 months'
        p.payment_entered between '2025-04-01' and '2026-03-31'

        -- exclude null/blank practice name
        AND p.practice_name IS NOT NULL
        AND TRIM(p.practice_name) <> ''

        -- exclude null/blank credit payer
        AND p.credit_payer_name IS NOT NULL
        AND TRIM(p.credit_payer_name) <> ''
),

enriched AS (
    SELECT
        b.*,

        -- 4. Level of Care
        COALESCE(loc1.level_of_care, loc2.level_of_care) AS loc,

        -- 5. Payer Class
        pnc.payer_code AS payer_class,

        -- 6. Status
        CASE
            WHEN b.int_charge_balance_due_ins > 0 THEN 'DO NOT INCLUDE'
            ELSE 'INCLUDE'
        END AS status

    FROM base b
    LEFT JOIN dw_combined.loc_crosswalk loc1
        ON loc1.rev_code::text = b.charge_rev_code::text
    LEFT JOIN dw_combined.loc_crosswalk loc2
        ON loc2.rev_code::text = b.charge_cpt_code
    LEFT JOIN dw_combined.payer_name_crosswalk pnc
        ON pnc.payer_name = b.credit_payer_name
)

SELECT
    customer_account,
    instance_key,
    facility_name,
    practice_name,
    location_code,
    charge_cpt_code,
    charge_rev_code,
    loc,
    charge_amount,
    payment_allowed_amount,
    primary_group_number,
    primary_member_id,
    credit_payer_name,
    payer_class,
    replace(replace(insurance_paid_amount::text, '$', ''), ',', '')::numeric as insurance_paid_amount,
    patient_paid_amount_w_copays,
    total_payment_received,
    payment_received,
    payment_entered,
    charge_from_date,
    charge_to_date,
    primary_ins_zip,
    primary_ins_city,
    primary_ins_state,
    primary_ins_addr_1,
    patient_zip,
    patient_city,
    patient_state,
    patient_address_1,
    charge_balance_due_ins,
    status,
    charge_balance_due_pat,
    charge_balance_at_collections,

    -- Unique ID
    CONCAT(
        REGEXP_REPLACE(location_code, '\s+', '', 'g'),
        REGEXP_REPLACE(payer_class, '\s+', '', 'g'),
        REGEXP_REPLACE(loc, '\s+', '', 'g')
         ) AS unique_id,

    created_at

FROM enriched;

CREATE INDEX IF NOT EXISTS idx_pdr3_calculator_account ON pdr3_calculator_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_pdr3_calculator_entered ON pdr3_calculator_view(payment_entered);
CREATE INDEX IF NOT EXISTS idx_pdr3_calculator_instance_key ON pdr3_calculator_view(instance_key);
-- Index the Unique ID used for Tableau relationships
CREATE INDEX IF NOT EXISTS idx_pdr3_calculator_unique_id ON pdr3_calculator_view(unique_id);


DROP MATERIALIZED VIEW IF EXISTS rev_rec_payments_view;
DROP MATERIALIZED VIEW IF EXISTS rev_rec_charges_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS rev_rec_charges_base_view CASCADE;
DROP MATERIALIZED VIEW IF EXISTS pdr3_global_rate_card_view;
CREATE MATERIALIZED VIEW pdr3_global_rate_card_view AS
SELECT
    p.practice_name,
    p.facility_name,
    p.instance_key,
    p.location_code,
    p.loc,
    p.payer_class,
    p.unique_id,
    fr.rate,
    fr.inn_oon,

    -- rate calculation
    ROUND(AVG(p.total_payment_received), 2) AS revenue_recognized,
    (
        CASE
        WHEN ROUND(AVG(p.total_payment_received), 2) > fr.rate OR ROUND(AVG(p.total_payment_received), 2) < 0.4 * fr.rate
        THEN 'Needs Review'
        ELSE 'OK'
        END
    ) AS flaged,

    -- optional: label the period (recommended)
    CONCAT('Q', EXTRACT(QUARTER FROM ('2026-03-31'::date)), ' ', EXTRACT(YEAR FROM ('2026-03-31'::date))) AS rate_period,

    NOW() AT TIME ZONE 'MST' AS rate_calculated_at

FROM pdr3_calculator_view p
JOIN facility_rates fr ON
            p.unique_id = fr.unique_id
-- only include usable rows (important)
WHERE p.status = 'INCLUDE' AND upper(p.charge_rev_code) NOT IN ('INT', 'INTEREST',  'INTCHRG')

GROUP BY
    p.practice_name,
    p.instance_key,
    p.location_code,
    p.loc,
    p.payer_class,
    p.unique_id, p.facility_name, fr.inn_oon, fr.rate;

CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_unique_id ON pdr3_global_rate_card_view(unique_id);
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_practice ON pdr3_global_rate_card_view(practice_name) ;
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_location ON pdr3_global_rate_card_view(location_code);
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_payer_class ON pdr3_global_rate_card_view(payer_class);
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_loc ON pdr3_global_rate_card_view(loc);
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_instance_key ON pdr3_global_rate_card_view(instance_key);

----
CREATE MATERIALIZED VIEW rev_rec_charges_view AS
WITH base AS (
    SELECT
        rrc.customer_account::varchar                                                                  AS customer_account,
        rrc.instance_key,
        rrc.practice_name                                                                             AS office_name,
        rrc.practice_name                                                                             AS facility_name,
        rrc.practice_name,
        rrc.charge_primary_payer_name,
        rrc.charge_primary_payer_id::varchar                                                          AS charge_primary_payer_id,
        rrc.claim_primary_payer_type,
        rrc.charge_secondary_payer_name,
        rrc.claim_type,
        rrc.charge_patient_id::varchar                                                                AS charge_patient_id,
        rrc.charge_id::varchar                                                                        AS charge_id,
        rrc.claim_id::varchar                                                                         AS claim_id,
        rrc.patient_full_name,
        rrc.primary_payer_member_id,
        rrc.charge_from_date,
        rrc.charge_to_date,
        rrc.claim_first_billed_date,
        rrc.times_billed,
        rrc.admission_date,
        rrc.claim_admit_code,
        rrc.charge_cpt_code,
        rrc.charge_rev_code,
        rrc.charge_units_sum::varchar                                                                 AS charge_units_sum,
        rrc.type_of_bill::varchar                                                                     AS type_of_bill,
        rrc.claim_status,
        rrc.charge_amount,
        rrc.primary_group,
        rrc.claim_primary_member_id,
        split_part(rrc.practice_name, ' ', 1)                                                        AS location_code,
        rrc.created_at,
        ltrim(split_part(regexp_replace(rrc.charge_rev_code::text, '\.0$', ''), ' ', 1), '0')        AS clean_rev_code,
        ltrim(split_part(regexp_replace(rrc.charge_cpt_code::text, '\.0$', ''), ' ', 1), '0')        AS clean_cpt_code,
        replace(replace(rrc.charge_amount::text, '$', ''), ',', '')::numeric                         AS int_charge_amount,
        to_char(rrc.claim_first_billed_date::date::timestamptz, 'day')                               AS claim_first_billed_day,
        'Week' || to_char(rrc.claim_first_billed_date::date::timestamptz, 'IW')                      AS claim_first_billed_week,
        to_char(rrc.claim_first_billed_date::date::timestamptz, 'Month')                             AS claim_first_billed_month,
        to_char(rrc.claim_first_billed_date::date::timestamptz, 'YYYY')                              AS claim_first_billed_year
    FROM rev_rec_charges rrc
),
enriched AS (
    SELECT
        b.*,
        coalesce(loc1.level_of_care, loc2.level_of_care)                                             AS loc,
        pnc.payer_code                                                                               AS payer_class,
        CONCAT(
            regexp_replace(split_part(b.practice_name, ' ', 1), '\s+', '', 'g'),
            regexp_replace(pnc.payer_code,                                        '\s+', '', 'g'),
            regexp_replace(coalesce(loc1.level_of_care, loc2.level_of_care),     '\s+', '', 'g')
        )                                                                                            AS unique_id,
        CONCAT(b.charge_id, b.claim_id, b.practice_name)                                            AS abs_unique_id
    FROM base b
    LEFT JOIN loc_crosswalk        loc1 ON loc1.rev_code  = b.clean_rev_code
    LEFT JOIN loc_crosswalk        loc2 ON loc2.rev_code  = b.clean_cpt_code
    LEFT JOIN payer_name_crosswalk pnc  ON pnc.payer_name = b.charge_primary_payer_name
)
SELECT
    e.*,
    fr.rate,
    fr.inn_oon,
    p3.revenue_recognized
FROM enriched e
LEFT JOIN facility_rates fr
    ON e.unique_id = fr.unique_id
LEFT JOIN pdr3_global_rate_card_view p3 ON p3.unique_id = e.unique_id;

-- Index on the materialized view for fast lookups
CREATE INDEX ON rev_rec_charges_view (unique_id);
CREATE INDEX ON rev_rec_charges_view (abs_unique_id);

----


CREATE MATERIALIZED VIEW rev_rec_payments_view AS
WITH rev_rec_payments_cte AS (
    SELECT
        rrp.customer_account::varchar AS customer_account,
        rrp.instance_key,
        rrp.practice_name AS office_name,
        rrp.practice_name AS facility_name,
        rrp.practice_name,
        rrp.charge_cpt_code,
        rrp.charge_rev_code::text AS charge_rev_code,
        rrp.charge_primary_payer_name,
        rrp.charge_id::varchar AS charge_id,
        rrp.charge_claim_id::varchar AS charge_claim_id,
        rrp.charge_amount,
        rrp.payment_allowed_amount,
        rrp.primary_group_number,
        rrp.primary_member_id,
        rrp.credit_payer_name,
        rrp.payment_total_paid,
        rrp.payment_received,
        rrp.payment_entered,
        rrp.charge_from_date,
        rrp.charge_to_date,
        rrp.created_at,
        split_part(rrp.practice_name, ' ', 1) AS location_code,
        -- cleaned codes for LOC lookup
        ltrim(split_part(regexp_replace(rrp.charge_rev_code::text, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(rrp.charge_cpt_code::text, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        replace(replace(rrp.charge_amount::text, '$', ''), ',', '')::numeric AS int_charge_amount,
        replace(replace(rrp.payment_allowed_amount::text, '$', ''), ',', '')::numeric AS int_payment_allowed_amount,
        replace(replace(rrp.payment_total_paid::text, '$', ''), ',', '')::numeric AS int_payment_total_paid,
        to_char(rrp.payment_received::date::timestamp with time zone, 'day') AS payment_received_day,
        'Week' || to_char(rrp.payment_received::date::timestamp with time zone, 'IW') AS payment_received_week,
        to_char(rrp.payment_received::date::timestamp with time zone, 'Month') AS payment_received_month,
        to_char(rrp.payment_received::date::timestamp with time zone, 'YYYY') AS payment_received_year
    FROM rev_rec_payments rrp
),
enriched AS (
    SELECT
        r.*,
        coalesce(loc1.level_of_care, loc2.level_of_care) AS loc,
        pnc.payer_code AS payer_class
    FROM rev_rec_payments_cte r
    LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
    LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code
    LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = r.charge_primary_payer_name
),
unique_enriched AS
         (SELECT *,
                 -- Unique ID
                 CONCAT(
                         REGEXP_REPLACE(location_code, '\s+', '', 'g'),
                         REGEXP_REPLACE(payer_class, '\s+', '', 'g'),
                         REGEXP_REPLACE(loc, '\s+', '', 'g')
                 ) AS unique_id,
                 -- Absolute Unique ID: charge_id + charge_claim_id + practice_name
                 CONCAT(charge_id, charge_claim_id, practice_name) AS abs_unique_id
          FROM enriched)
SELECT *
FROM unique_enriched;

CREATE INDEX IF NOT EXISTS idx_rev_rec_payments_account ON rev_rec_payments_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_rev_rec_payments_instance_key ON rev_rec_payments_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_rev_rec_payments_received_date ON rev_rec_payments_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_rev_rec_payments_unique_id ON rev_rec_payments_view(unique_id);

----

DROP MATERIALIZED VIEW IF EXISTS denial_trends_view;
CREATE MATERIALIZED VIEW denial_trends_view AS
WITH denial_trends_cte AS (
    SELECT
        dt.customer_account::varchar AS customer_account,
        dt.instance_key,
        dt.facility_name,
        dt.practice_name,
        dt.office_name,
        dt.charge_entered_date,
        dt.charge_from_date,
        dt.charge_to_date,
        dt.charge_first_bill_date,
        dt.patient_id::varchar AS patient_id,
        dt.patient_full_name,
        dt.charge_id::varchar AS charge_id,
        dt.charge_rev_code,
        dt.charge_cpt_code,
        dt.remark_code_s,
        dt.unpaid_reason_code_s,
        dt.charge_primary_payment_date,
        dt.payment_received,
        dt.payment_entered,
        dt.charge_primary_payer_name,
        dt.payer_name,
        dt.created_at,
        -- cleaned codes for LOC lookup
        ltrim(split_part(regexp_replace(dt.charge_rev_code, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(dt.charge_cpt_code, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        -- numeric cleaning
        replace(replace(dt.charge_amount::text, '$', ''), ',', '')::numeric AS int_charge_amount,
        replace(replace(dt.insurance_paid_amount::text, '$', ''), ',', '')::numeric AS int_insurance_paid_amount,
        -- date calculations
        (NOW() AT TIME ZONE 'MST')::date - dt.charge_entered_date::date AS days_on_hold,
        to_char(dt.payment_received::date::timestamp with time zone, 'day') AS payment_received_day,
        'Week' || to_char(dt.payment_received::date::timestamp with time zone, 'IW') AS payment_received_week,
        to_char(dt.payment_received::date::timestamp with time zone, 'Month') AS payment_received_month,
        to_char(dt.payment_received::date::timestamp with time zone, 'YYYY') AS payment_received_year
    FROM denial_trends dt
)
SELECT
    customer_account,
    instance_key,
    facility_name,
    practice_name,
    office_name,
    charge_entered_date,
    charge_from_date,
    charge_to_date,
    charge_first_bill_date,
    patient_id,
    patient_full_name,
    charge_id,
    charge_rev_code,
    charge_cpt_code,
    remark_code_s,
    unpaid_reason_code_s,
    charge_primary_payment_date,
    payment_received,
    payment_entered,
    charge_primary_payer_name,
    denial_trends_cte.payer_name,
    created_at,
    int_charge_amount,
    int_insurance_paid_amount,
    coalesce(loc1.level_of_care, loc2.level_of_care) AS loc,
    pnc.payer_code AS payer_class,
    days_on_hold,
    CASE
        WHEN days_on_hold <= 5 THEN '0-5 days'
        WHEN days_on_hold > 5 AND days_on_hold <= 10 THEN '6-10 days'
        WHEN days_on_hold > 10 AND days_on_hold < 15 THEN '11-14 days'
        WHEN days_on_hold >= 15 AND days_on_hold <= 21 THEN '15-21 days'
        WHEN days_on_hold > 21 AND days_on_hold <= 30 THEN '22-30 days'
        WHEN days_on_hold > 30 AND days_on_hold <= 45 THEN '31-45 days'
        WHEN days_on_hold > 45 AND days_on_hold <= 60 THEN '46-60 days'
        WHEN days_on_hold > 60 AND days_on_hold <= 90 THEN '61-90 days'
        WHEN days_on_hold > 90 THEN 'over 90 days'
        ELSE NULL
    END AS days_on_hold_range,
    payment_received_day,
    payment_received_week,
    payment_received_month,
    payment_received_year,
    -- Unique ID consistent with revenue recognition views
    CONCAT(
        REGEXP_REPLACE(split_part(practice_name, ' ', 1), '\s+', '', 'g'),
        REGEXP_REPLACE(pnc.payer_code, '\s+', '', 'g'),
        REGEXP_REPLACE(coalesce(loc1.level_of_care, loc2.level_of_care), '\s+', '', 'g')
    ) AS unique_id
FROM denial_trends_cte
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = charge_primary_payer_name;

CREATE INDEX IF NOT EXISTS idx_denial_trends_account ON denial_trends_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_denial_trends_instance_key ON denial_trends_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_denial_trends_received_date ON denial_trends_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_denial_trends_entered_date ON denial_trends_view(charge_entered_date);

----

DROP MATERIALIZED VIEW IF EXISTS claim_stage_breakdown_view;
CREATE MATERIALIZED VIEW claim_stage_breakdown_view AS
WITH claim_stage_cte AS (
    SELECT
        csb.customer_account::varchar AS customer_account,
        csb.instance_key,
        csb.practice_name,
        csb.facility_name,
        csb.office_name,
        csb.patient_id::varchar AS patient_id,
        csb.claim_id::varchar AS claim_id,
        csb.charge_id::varchar AS charge_id,
        regexp_replace(csb.patient_full_name, '\s*\(\d+\)$', '') AS patient_full_name,
        csb.charge_from_date,
        csb.charge_to_date,
        csb.charge_entered_date,
        csb.type_of_bill::varchar AS type_of_bill,
        csb.charge_cpt_code,
        csb.charge_rev_code::text AS charge_rev_code,
        csb.charge_primary_payer_name,
        csb.charge_current_payer_name,
        csb.claim_status,
        csb.created_at,
        -- cleaned codes for LOC lookup
        ltrim(split_part(regexp_replace(csb.charge_rev_code::text, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(csb.charge_cpt_code, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        -- numeric cleaning for currency
        replace(replace(csb.charge_amount, '$', ''), ',', '')::numeric AS int_charge_amount,
        replace(replace(csb.charge_balance, '$', ''), ',', '')::numeric AS int_charge_balance,
        replace(replace(csb.charge_balance_due_ins, '$', ''), ',', '')::numeric AS int_charge_balance_due_ins,
        replace(replace(csb.charge_balance_due_other, '$', ''), ',', '')::numeric AS int_charge_balance_due_other,
        replace(replace(csb.charge_balance_due_pat, '$', ''), ',', '')::numeric AS int_charge_balance_due_pat,
        replace(replace(csb.charge_balance_at_collections, '$', ''), ',', '')::numeric AS int_charge_balance_at_collections,
        -- date logic
        (NOW() AT TIME ZONE 'MST')::date - csb.charge_entered_date::date AS days_on_hold,
        to_char(csb.charge_entered_date::date::timestamp with time zone, 'day') AS charge_entered_day,
        'Week' || to_char(csb.charge_entered_date::date::timestamp with time zone, 'IW') AS charge_entered_week,
        to_char(csb.charge_entered_date::date::timestamp with time zone, 'Month') AS charge_entered_month,
        to_char(csb.charge_entered_date::date::timestamp with time zone, 'YYYY') AS charge_entered_year
    FROM claim_stage_breakdown csb
)
SELECT
    cs.*,
    coalesce(loc1.level_of_care, loc2.level_of_care) AS loc,
    pnc.payer_code AS payer_class,
    CASE
        WHEN days_on_hold <= 5 THEN '0-5 days'
        WHEN days_on_hold > 5 AND days_on_hold <= 10 THEN '6-10 days'
        WHEN days_on_hold > 10 AND days_on_hold < 15 THEN '11-14 days'
        WHEN days_on_hold >= 15 AND days_on_hold <= 21 THEN '15-21 days'
        WHEN days_on_hold > 21 AND days_on_hold <= 30 THEN '22-30 days'
        WHEN days_on_hold > 30 AND days_on_hold <= 45 THEN '31-45 days'
        WHEN days_on_hold > 45 AND days_on_hold <= 60 THEN '46-60 days'
        WHEN days_on_hold > 60 AND days_on_hold <= 90 THEN '61-90 days'
        WHEN days_on_hold > 90 THEN 'over 90 days'
        ELSE NULL
    END AS days_on_hold_range,
    -- Unique ID
    CONCAT(
        REGEXP_REPLACE(split_part(practice_name, ' ', 1), '\s+', '', 'g'),
        REGEXP_REPLACE(pnc.payer_code, '\s+', '', 'g'),
        REGEXP_REPLACE(coalesce(loc1.level_of_care, loc2.level_of_care), '\s+', '', 'g')
    ) AS unique_id
FROM claim_stage_cte cs
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = cs.charge_primary_payer_name;

CREATE INDEX IF NOT EXISTS idx_claim_stage_account ON claim_stage_breakdown_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_claim_stage_instance_key ON claim_stage_breakdown_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_claim_stage_entered_date ON claim_stage_breakdown_view(charge_entered_date);
CREATE INDEX IF NOT EXISTS idx_claim_stage_unique_id ON claim_stage_breakdown_view(unique_id);

----

DROP MATERIALIZED VIEW IF EXISTS quadrant_performance_view;
CREATE MATERIALIZED VIEW quadrant_performance_view AS
WITH qp_cte AS (
    SELECT
        qp.customer_account::varchar AS customer_account,
        qp.instance_key,
        qp.facility_name,
        qp.office_name,
        qp.practice_name,
        qp.charge_entered_date,
        qp.charge_from_date,
        qp.charge_to_date,
        qp.patient_full_name,
        qp.payment_source,
        qp.charge_patient_id::bigint::varchar AS charge_patient_id,
        qp.charge_id::bigint::varchar AS charge_id,
        qp.charge_claim_id::bigint::varchar AS charge_claim_id,
        qp.payer_name,
        qp.primary_payer_member_id,
        qp.payment_received,
        qp.payment_entered,
        qp.primary_ins_zip,
        qp.primary_ins_city,
        qp.primary_ins_state,
        qp.primary_ins_addr_1,
        qp.patient_zip,
        qp.patient_city,
        qp.patient_state,
        qp.patient_address_1,
        qp.created_at,
        qp.type_of_bill::varchar AS type_of_bill,
        -- cleaned codes for LOC lookup
        ltrim(split_part(regexp_replace(qp.charge_rev_code::text, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(qp.charge_cpt_code, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        -- numeric cleaning
        replace(replace(qp.payment_allowed_amount, '$', ''), ',', '')::numeric AS int_payment_allowed_amount,
        replace(replace(qp.charge_amount, '$', ''), ',', '')::numeric AS int_charge_amount,
        replace(replace(qp.insurance_paid_amount, '$', ''), ',', '')::numeric AS int_insurance_paid_amount,
        replace(replace(qp.payment_total_paid, '$', ''), ',', '')::numeric AS int_payment_total_paid,
        replace(replace(qp.payment_total_applied, '$', ''), ',', '')::numeric AS int_payment_total_applied,
        replace(replace(qp.charge_insurance_adjustments, '$', ''), ',', '')::numeric AS int_charge_insurance_adjustments,
        replace(replace(qp.charge_patient_adjustments, '$', ''), ',', '')::numeric AS int_charge_patient_adjustments,
        replace(replace(qp.charge_total_adjustments, '$', ''), ',', '')::numeric AS int_charge_total_adjustments,
        replace(replace(qp.insurance_applied_amount, '$', ''), ',', '')::numeric AS int_insurance_applied_amount,
        replace(replace(qp.patient_applied_amount, '$', ''), ',', '')::numeric AS int_patient_applied_amount,
        replace(replace(qp.payment_applied_amount, '$', ''), ',', '')::numeric AS int_payment_applied_amount,
        replace(replace(qp.payment_unapplied_amount, '$', ''), ',', '')::numeric AS int_payment_unapplied_amount,
        -- date dimensions
        (NOW() AT TIME ZONE 'MST')::date - qp.charge_entered_date::date AS days_on_hold,
        to_char(qp.payment_received::date::timestamp with time zone, 'day') AS payment_received_day,
        'Week' || to_char(qp.payment_received::date::timestamp with time zone, 'IW') AS payment_received_week,
        to_char(qp.payment_received::date::timestamp with time zone, 'Month') AS payment_received_month,
        to_char(qp.payment_received::date::timestamp with time zone, 'YYYY') AS payment_received_year
    FROM quadrant_performance qp
)
SELECT
    qp_cte.customer_account,
    qp_cte.instance_key,
    qp_cte.facility_name,
    qp_cte.office_name,
    qp_cte.practice_name,
    qp_cte.charge_entered_date,
    qp_cte.charge_from_date,
    qp_cte.charge_to_date,
    qp_cte.patient_full_name,
    qp_cte.payment_source,
    qp_cte.charge_patient_id,
    qp_cte.charge_id,
    qp_cte.charge_claim_id,
    qp_cte.payer_name,
    qp_cte.primary_payer_member_id,
    qp_cte.payment_received,
    qp_cte.payment_entered,
    qp_cte.primary_ins_zip,
    qp_cte.primary_ins_city,
    qp_cte.primary_ins_state,
    qp_cte.primary_ins_addr_1,
    qp_cte.patient_zip,
    qp_cte.patient_city,
    qp_cte.patient_state,
    qp_cte.patient_address_1,
    qp_cte.created_at,
    qp_cte.type_of_bill,
    qp_cte.int_payment_allowed_amount,
    qp_cte.int_charge_amount,
    qp_cte.int_insurance_paid_amount,
    qp_cte.int_payment_total_paid,
    qp_cte.int_payment_total_applied,
    qp_cte.int_charge_insurance_adjustments,
    qp_cte.int_charge_patient_adjustments,
    qp_cte.int_charge_total_adjustments,
    qp_cte.int_insurance_applied_amount,
    qp_cte.int_patient_applied_amount,
    qp_cte.int_payment_applied_amount,
    qp_cte.int_payment_unapplied_amount,
    qp_cte.days_on_hold,
    qp_cte.payment_received_day,
    qp_cte.payment_received_week,
    qp_cte.payment_received_month,
    qp_cte.payment_received_year,
    coalesce(loc1.level_of_care, loc2.level_of_care) AS loc,
    pnc.payer_code AS payer_class,
    -- Unique ID consistent with rate card views
    CONCAT(
        REGEXP_REPLACE(split_part(qp_cte.practice_name, ' ', 1), '\s+', '', 'g'),
        REGEXP_REPLACE(pnc.payer_code, '\s+', '', 'g'),
        REGEXP_REPLACE(coalesce(loc1.level_of_care, loc2.level_of_care), '\s+', '', 'g')
    ) AS unique_id
FROM qp_cte
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = qp_cte.clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = qp_cte.clean_cpt_code
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = qp_cte.payer_name;

CREATE INDEX IF NOT EXISTS idx_quadrant_perf_account ON quadrant_performance_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_quadrant_perf_instance_key ON quadrant_performance_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_quadrant_perf_received_date ON quadrant_performance_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_quadrant_perf_unique_id ON quadrant_performance_view(unique_id);

----

DROP MATERIALIZED VIEW IF EXISTS rcm_productivity_view;
CREATE MATERIALIZED VIEW rcm_productivity_view AS
WITH rcm_prod_cte AS (
    SELECT
        rp.customer_account::varchar AS customer_account,
        rp.instance_key,
        rp.created_by_user,
        rp.practice_name,
        rp.office_name,
        rp.note_count_cntunq,
        rp.claim_id::varchar AS claim_id,
        rp.claim_first_billed_date,
        rp.claim_last_billed_date,
        rp.last_note_date_max,
        rp.claim_from_date,
        rp.claim_to_date,
        rp.claim_status,
        rp.created_at,
        -- Productivity Date Dimensions (Billing)
        initcap(to_char(rp.claim_first_billed_date::date::timestamp with time zone, 'day')) AS billed_day,
        'Week' || to_char(rp.claim_first_billed_date::date::timestamp with time zone, 'IW') AS billed_week,
        to_char(rp.claim_first_billed_date::date::timestamp with time zone, 'Month') AS billed_month,
        to_char(rp.claim_first_billed_date::date::timestamp with time zone, 'YYYY') AS billed_year,
        -- Productivity Date Dimensions (Notes)
        initcap(to_char(rp.last_note_date_max::date::timestamp with time zone, 'day')) AS last_note_day,
        'Week' || to_char(rp.last_note_date_max::date::timestamp with time zone, 'IW') AS last_note_week,
        to_char(rp.last_note_date_max::date::timestamp with time zone, 'Month') AS last_note_month,
        to_char(rp.last_note_date_max::date::timestamp with time zone, 'YYYY') AS last_note_year,
        -- Lag Calculations
        (NOW() AT TIME ZONE 'MST')::date - rp.last_note_date_max::date AS days_since_last_action,
        rp.last_note_date_max::date - rp.claim_first_billed_date::date AS billing_to_note_lag_days
    FROM rcm_productivity rp
)
SELECT
    *,
    CASE
        WHEN billing_to_note_lag_days < 0 THEN 'Action Before Billing'
        WHEN billing_to_note_lag_days <= 7 THEN '0-7 Days'
        WHEN billing_to_note_lag_days <= 14 THEN '8-14 Days'
        WHEN billing_to_note_lag_days <= 30 THEN '15-30 Days'
        WHEN billing_to_note_lag_days > 30 THEN 'Over 30 Days'
        ELSE 'No Action Recorded'
    END AS follow_up_efficiency_bucket,
    CASE
        WHEN note_count_cntunq = 0 THEN 'Unworked'
        WHEN note_count_cntunq = 1 THEN 'Single Touch'
        WHEN note_count_cntunq BETWEEN 2 AND 5 THEN '2-5 Touches'
        WHEN note_count_cntunq > 5 THEN 'High Touch'
        ELSE 'Unknown'
    END AS touch_point_category
FROM rcm_prod_cte;

CREATE INDEX IF NOT EXISTS idx_rcm_prod_account ON rcm_productivity_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_instance_key ON rcm_productivity_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_user ON rcm_productivity_view(created_by_user);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_claim_id ON rcm_productivity_view(claim_id);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_billed_date ON rcm_productivity_view(claim_first_billed_date);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_note_date ON rcm_productivity_view(last_note_date_max);

----

DROP MATERIALIZED VIEW IF EXISTS write_off_trend_view;
CREATE MATERIALIZED VIEW write_off_trend_view AS
WITH wot_cte AS (
    SELECT
        wot.customer_account::varchar AS customer_account,
        wot.instance_key,
        wot.practice_name,
        wot.facility_name,
        wot.office_name,
        wot.charge_entered_date,
        wot.claim_first_billed_date,
        wot.charge_from_date,
        wot.charge_to_date,
        wot.payment_username,
        wot.patient_id::varchar AS patient_id,
        wot.charge_id::bigint::varchar AS charge_id,
        wot.charge_claim_id::varchar AS charge_claim_id,
        wot.payment_received,
        regexp_replace(wot.patient_full_name, '\s*\(\d+\)$', '') AS patient_full_name,
        wot.credit_source_w_o_payer,
        wot.adjustment_code_s,
        wot.credit_payer_name,
        wot.payment_payer_id::varchar AS payment_payer_id,
        wot.created_at,
        -- numeric cleaning for currency
        replace(replace(wot.patient_total_credits, '$', ''), ',', '')::numeric AS int_patient_total_credits,
        replace(replace(wot.payment_total_applied, '$', ''), ',', '')::numeric AS int_payment_total_applied,
        replace(replace(wot.patient_ins_credits, '$', ''), ',', '')::numeric AS int_patient_ins_credits,
        replace(replace(wot.patient_credits, '$', ''), ',', '')::numeric AS int_patient_credits,
        replace(replace(wot.patient_total_credits_1, '$', ''), ',', '')::numeric AS int_patient_total_credits_1,
        -- date logic
        (NOW() AT TIME ZONE 'MST')::date - wot.charge_entered_date::date AS days_on_hold,
        to_char(wot.payment_received::date::timestamp with time zone, 'day') AS payment_received_day,
        'Week' || to_char(wot.payment_received::date::timestamp with time zone, 'IW') AS payment_received_week,
        to_char(wot.payment_received::date::timestamp with time zone, 'Month') AS payment_received_month,
        to_char(wot.payment_received::date::timestamp with time zone, 'YYYY') AS payment_received_year
    FROM write_off_trend wot
)
SELECT
    wc.*,
    pnc.payer_code AS payer_class,
    CASE
        WHEN days_on_hold <= 5 THEN '0-5 days'
        WHEN days_on_hold > 5 AND days_on_hold <= 10 THEN '6-10 days'
        WHEN days_on_hold > 10 AND days_on_hold < 15 THEN '11-14 days'
        WHEN days_on_hold >= 15 AND days_on_hold <= 21 THEN '15-21 days'
        WHEN days_on_hold > 21 AND days_on_hold <= 30 THEN '22-30 days'
        WHEN days_on_hold > 30 AND days_on_hold <= 45 THEN '31-45 days'
        WHEN days_on_hold > 45 AND days_on_hold <= 60 THEN '46-60 days'
        WHEN days_on_hold > 60 AND days_on_hold <= 90 THEN '61-90 days'
        WHEN days_on_hold > 90 THEN 'over 90 days'
        ELSE NULL
    END AS days_on_hold_range
FROM wot_cte wc
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = wc.credit_payer_name;

CREATE INDEX IF NOT EXISTS idx_write_off_trend_account ON write_off_trend_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_write_off_trend_instance_key ON write_off_trend_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_write_off_trend_received_date ON write_off_trend_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_write_off_trend_entered_date ON write_off_trend_view(charge_entered_date);