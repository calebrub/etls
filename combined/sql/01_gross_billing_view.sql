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
        ltrim(split_part(regexp_replace(gb.charge_rev_code::text, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
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
    gross_billing_cte.customer_account,
    gross_billing_cte.office_name,
    gross_billing_cte.facility_name,
    gross_billing_cte.practice_name,
    gross_billing_cte.charge_primary_payer_name,
    gross_billing_cte.charge_patient_id,
    gross_billing_cte.charge_id,
    gross_billing_cte.charge_claim_id,
    gross_billing_cte.patient_full_name,
    gross_billing_cte.primary_payer_member_id,
    gross_billing_cte.charge_from_date,
    gross_billing_cte.charge_to_date,
    gross_billing_cte.charge_entered_date,
    gross_billing_cte.type_of_bill,
    gross_billing_cte.claim_first_billed_date,
    gross_billing_cte.charge_cpt_code,
    gross_billing_cte.charge_units_sum,
    gross_billing_cte.charge_amount,
    gross_billing_cte.charge_rev_code,
    gross_billing_cte.charge_current_payer_name,
    gross_billing_cte.claim_status,
    gross_billing_cte.date_charge_entered_date,
    gross_billing_cte.days_on_hold,
    gross_billing_cte.charge_lag,
    gross_billing_cte.billed_or_not_billed,
    gross_billing_cte.claim_first_billed_day,
    gross_billing_cte.claim_first_billed_week,
    gross_billing_cte.claim_first_billed_month,
    gross_billing_cte.claim_first_billed_year,
    coalesce(loc1.level_of_care, loc2.level_of_care) AS loc,
    gross_billing_cte.int_charge_amount,
    gross_billing_cte.first_billed_week_date,
    CASE
        WHEN gross_billing_cte.days_on_hold <= 5 THEN '0-5 days'
        WHEN gross_billing_cte.days_on_hold > 5 AND gross_billing_cte.days_on_hold <= 10 THEN '6-10 days'
        WHEN gross_billing_cte.days_on_hold > 10 AND gross_billing_cte.days_on_hold < 15 THEN '11-14 days'
        WHEN gross_billing_cte.days_on_hold >= 15 AND gross_billing_cte.days_on_hold <= 21 THEN '15-21 days'
        WHEN gross_billing_cte.days_on_hold > 21 AND gross_billing_cte.days_on_hold <= 30 THEN '22-30 days'
        WHEN gross_billing_cte.days_on_hold > 30 AND gross_billing_cte.days_on_hold <= 45 THEN '31-45 days'
        WHEN gross_billing_cte.days_on_hold > 45 AND gross_billing_cte.days_on_hold <= 60 THEN '46-60 days'
        WHEN gross_billing_cte.days_on_hold > 60 AND gross_billing_cte.days_on_hold <= 90 THEN '61-90 days'
        WHEN gross_billing_cte.days_on_hold > 90 THEN 'over 90 days'
        ELSE NULL
    END AS days_on_hold_range,
    concat(gross_billing_cte.claim_first_billed_month, '', gross_billing_cte.claim_first_billed_year) AS claim_first_billed_ym,
    gross_billing_cte.instance_key,
    gross_billing_cte.claim_first_billed_date_cln,
    gross_billing_cte.int_charge_amount_cln,
    pnc.payer_code AS payer_class,
    CONCAT(
        REGEXP_REPLACE(split_part(gross_billing_cte.practice_name, ' ', 1), '\s+', '', 'g'),
        REGEXP_REPLACE(pnc.payer_code, '\s+', '', 'g'),
        REGEXP_REPLACE(coalesce(loc1.level_of_care, loc2.level_of_care), '\s+', '', 'g')
    ) AS unique_id,
    COALESCE(fr.inn_oon, 'OON') AS inn_oon,
    COALESCE(fr.inn_oon, 'OON') AS network_status
FROM gross_billing_cte
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = gross_billing_cte.charge_primary_payer_name
LEFT JOIN facility_rates fr ON fr.unique_id = CONCAT(
    REGEXP_REPLACE(split_part(gross_billing_cte.practice_name, ' ', 1), '\s+', '', 'g'),
    REGEXP_REPLACE(pnc.payer_code, '\s+', '', 'g'),
    REGEXP_REPLACE(coalesce(loc1.level_of_care, loc2.level_of_care), '\s+', '', 'g')
);

CREATE INDEX IF NOT EXISTS idx_gross_billing_account ON gross_billing_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_gross_billing_entered_date ON gross_billing_view(charge_entered_date);
CREATE INDEX IF NOT EXISTS idx_gross_billing_billed_date ON gross_billing_view(claim_first_billed_date);
CREATE INDEX IF NOT EXISTS idx_gross_billing_instance_key ON gross_billing_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_gross_billing_unique_id ON gross_billing_view(unique_id);

REFRESH MATERIALIZED VIEW gross_billing_view;

