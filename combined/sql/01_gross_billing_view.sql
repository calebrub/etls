CREATE MATERIALIZED VIEW IF NOT EXISTS gross_billing_view AS
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
        NULLIF(replace(replace(gb.charge_amount::text, '$', ''), ',', ''), '')::numeric AS int_charge_amount,
        gb.claim_first_billed_date::date - EXTRACT(dow FROM gb.claim_first_billed_date::date)::integer AS first_billed_week_date,
        gb.instance_key,
        CASE
            WHEN gb.charge_primary_payer_name ILIKE 'self pay' THEN gb.charge_entered_date
            ELSE gb.claim_first_billed_date
        END AS claim_first_billed_date_cln,
        CASE
            WHEN gb.claim_first_billed_date IS NULL AND gb.charge_primary_payer_name NOT ILIKE 'self pay' THEN 0
            ELSE NULLIF(replace(replace(gb.charge_amount::text, '$', ''), ',', ''), '')::numeric
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

REFRESH MATERIALIZED VIEW gross_billing_view;
