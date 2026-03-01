CREATE OR REPLACE VIEW gross_billing_view AS
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
    int_charge_amount_cln
FROM gross_billing_cte
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code;


----


CREATE OR REPLACE VIEW payment_trends_view
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
             int_payment_total_paid, int_payment_unapplied_amount)
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
    pt.insurance_paid_amount,
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
    instance_key
FROM payment_trend pt
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = ltrim(split_part(regexp_replace(pt.charge_rev_code, '\.0$', ''), ' ', 1), '0')
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = ltrim(split_part(regexp_replace(pt.charge_cpt_code, '\.0$', ''), ' ', 1), '0');


----


CREATE OR REPLACE VIEW charges_on_hold_view AS
WITH charges_on_hold_cte AS (
    SELECT
        coh.customer_account::varchar AS customer_account,
        coh.instance_key,
        coh.practice_name,
        coh.facility_name,
        coh.office_name,
        coh.charge_primary_payer_type,
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
        CURRENT_DATE - coh.charge_entered_date::date AS days_on_hold,
        replace(replace(coh.charge_amount::text, '$', ''), ',', '')::numeric AS int_charge_amount
    FROM charges_on_hold coh
)
SELECT
    customer_account,
    instance_key,
    practice_name,
    facility_name,
    office_name,
    charge_primary_payer_type,
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
    int_charge_amount
FROM charges_on_hold_cte
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code;