CREATE MATERIALIZED VIEW IF NOT EXISTS ar_aging_view AS
WITH ar_aging_cte AS (
    SELECT
        aa.customer_account,
        aa.charge_id,
        aa.claim_id,
        aa.claim_status,
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
        aa.charge_amount,

        -- Numeric conversions
        NULLIF(replace(replace(aa.charge_balance_due_ins::text, '$', ''), ',', ''), '')::numeric AS int_charge_balance_due_ins,
        NULLIF(replace(replace(aa.charge_balance::text, '$', ''), ',', ''), '')::numeric AS int_charge_balance,
        NULLIF(replace(replace(aa.charge_balance_due_other::text, '$', ''), ',', ''), '')::numeric AS int_charge_balance_due_other,
        NULLIF(replace(replace(aa.charge_balance_due_pat::text, '$', ''), ',', ''), '')::numeric AS int_charge_balance_due_pat,
        NULLIF(replace(replace(aa.charge_amount::text, '$', ''), ',', ''), '')::numeric AS int_charge_amount,
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

REFRESH MATERIALIZED VIEW ar_aging_view;
