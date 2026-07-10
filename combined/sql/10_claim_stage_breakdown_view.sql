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
        NULLIF(replace(replace(csb.charge_amount, '$', ''), ',', ''), '')::numeric AS int_charge_amount,
        NULLIF(replace(replace(csb.charge_balance, '$', ''), ',', ''), '')::numeric AS int_charge_balance,
        NULLIF(replace(replace(csb.charge_balance_due_ins, '$', ''), ',', ''), '')::numeric AS int_charge_balance_due_ins,
        NULLIF(replace(replace(csb.charge_balance_due_other, '$', ''), ',', ''), '')::numeric AS int_charge_balance_due_other,
        NULLIF(replace(replace(csb.charge_balance_due_pat, '$', ''), ',', ''), '')::numeric AS int_charge_balance_due_pat,
        NULLIF(replace(replace(csb.charge_balance_at_collections, '$', ''), ',', ''), '')::numeric AS int_charge_balance_at_collections,
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
