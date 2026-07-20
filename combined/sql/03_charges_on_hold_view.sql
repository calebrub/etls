CREATE MATERIALIZED VIEW IF NOT EXISTS charges_on_hold_view AS
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
        ltrim(split_part(regexp_replace(coh.charge_rev_code::text, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(coh.charge_cpt_code, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        -- derived fields
        coh.charge_entered_date::date AS date_charge_entered,
        (NOW() AT TIME ZONE 'MST')::date - coh.charge_entered_date::date AS days_on_hold,
        NULLIF(replace(replace(coh.charge_amount::text, '$', ''), ',', ''), '')::numeric AS int_charge_amount,
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

REFRESH MATERIALIZED VIEW charges_on_hold_view;
