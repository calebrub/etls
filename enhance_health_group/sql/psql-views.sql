DROP VIEW IF EXISTS ar_aging_view;

CREATE VIEW ar_aging_view AS
WITH ar_aging_cte AS (
    SELECT
        aa.customer_account,
        aa.charge_id,
        aa.charge_claim_id AS claim_id,
        aa.charge_first_bill_date,
        aa.charge_from_date,
        aa.charge_to_date,
        aa.charge_entered_date,
        aa.charge_primary_payer_name AS charge_primary_payer_name,
        aa.charge_primary_payer_id,
        aa.practice_name,
        aa.practice_name AS office_name, -- alias for compatibility
        aa.charge_primary_payer_name AS charge_current_payer_name, -- approximate mapping
        aa.practice_name AS facility_name, -- alias for compatibility
        regexp_replace(aa.patient_name_id, '\s*\(\d+\)$', '') AS patient_full_name,
        regexp_replace(aa.patient_name_id, '.*\((\d+)\)$', '\1') AS claim_patient_id,
        aa.charge_cpt_code,
        aa.charge_cpt_description,
        aa.charge_billed_revenue_code AS revenue_code,
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
        -- numeric conversions
        replace(replace(aa.charge_balance_due_ins::text, '$', ''), ',', '')::numeric AS int_charge_balance_due_ins,
        replace(replace(aa.charge_balance_due_other::text, '$', ''), ',', '')::numeric AS int_charge_balance_due_other,
        aa.patient_stmts_sent_electronically::numeric AS int_patient_stmts_sent_electronically,
        aa.patient_statements_printed::numeric AS int_patient_statements_printed,
        replace(replace(aa.charge_balance_due_pat::text, '$', ''), ',', '')::numeric AS int_charge_balance_due_pat
    FROM ar_aging aa
)
SELECT
    ac.customer_account,
    ac.charge_id,
    ac.claim_id,
    ac.charge_first_bill_date,
    ac.charge_from_date,
    ac.charge_to_date,
    ac.charge_entered_date,
    ac.charge_primary_payer_name,
    ac.charge_primary_payer_id,
    ac.practice_name,
    ac.office_name,
    ac.charge_current_payer_name,
    ac.facility_name,
    ac.claim_patient_id,
    ac.charge_cpt_code,
    ac.charge_cpt_description,
    ac.revenue_code,
    ac.patient_full_name,
    ac.charge_fromdate_age,
    ac.charge_fromdate_age_days,
    ac.charge_first_bill_date_age,
    ac.charge_first_bill_date_age_days,
    ac.charge_balance,
    ac.charge_balance_due_ins,
    ac.charge_balance_due_other,
    ac.charge_balance_due_pat,
    ac.charge_balance_at_collections,
    ac.charge_insurance_payments,
    ac.charge_patient_payments,
    ac.charge_total_payments,
    ac.patient_stmts_sent_electronically,
    ac.patient_statements_printed,
    ac.int_charge_balance_due_ins,
    ac.int_charge_balance_due_other,
    ac.int_patient_stmts_sent_electronically,
    ac.int_patient_statements_printed,
    ac.int_charge_balance_due_pat,
    -- calculations
    ac.int_charge_balance_due_ins + ac.int_charge_balance_due_other AS balance_due_payer,
    ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically AS total_statements,
    CASE
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) < 3 THEN 'less than 3 statements'
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) BETWEEN 3 AND 4 THEN '3-4 statements'
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) BETWEEN 5 AND 6 THEN '5-6 statements'
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) > 6 THEN 'over 6 statements'
        ELSE NULL
    END AS total_statement_buckets,
    pc.payer_code
FROM ar_aging_cte ac
LEFT JOIN payer_name_crosswalk pc
    ON pc.payer_name::text = ac.charge_primary_payer_name;

-----
DROP VIEW IF EXISTS gross_billing_view;

CREATE OR REPLACE VIEW gross_billing_view AS
WITH gross_billing_cte AS (
    SELECT
        gb.customer_account::varchar AS customer_account,
        gb.practice_name AS office_name,                     -- V2: map practice_name
        gb.practice_name AS facility_name,                   -- V2: map practice_name
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
        gb.charge_primary_payer_name_1 AS charge_primary_payer_name_dup,
        gb.charge_current_payer_name,
        gb.claim_status,
        gb.charge_entered_date::date AS date_charge_entered_date,
        CURRENT_DATE - gb.charge_entered_date::date AS days_on_hold,
        gb.claim_first_billed_date::date - gb.charge_entered_date::date AS charge_lag,
        CASE
            WHEN gb.claim_first_billed_date IS NOT NULL THEN 'Billed'
            ELSE 'Not Billed'
        END AS billed_or_not_billed,
        to_char(gb.claim_first_billed_date::date::timestamp with time zone, 'day') AS claim_first_billed_day,
        'Week' || to_char(gb.claim_first_billed_date::date::timestamp with time zone, 'IW') AS claim_first_billed_week,
        to_char(gb.claim_first_billed_date::date::timestamp with time zone, 'Month') AS claim_first_billed_month,
        to_char(gb.claim_first_billed_date::date::timestamp with time zone, 'YYYY') AS claim_first_billed_year,
        lc.level_of_care AS loc,
        replace(replace(gb.charge_amount::text, '$', ''), ',', '')::numeric AS int_charge_amount,
        gb.claim_first_billed_date::date - EXTRACT(dow FROM gb.claim_first_billed_date::date)::integer AS first_billed_week_date
    FROM gross_billing gb
    LEFT JOIN loc_crosswalk lc ON lc.rev_code::text = gb.charge_rev_code::text
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
    charge_primary_payer_name_dup,
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
    loc,
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
    concat(claim_first_billed_month, '', claim_first_billed_year) AS claim_first_billed_ym
FROM gross_billing_cte;