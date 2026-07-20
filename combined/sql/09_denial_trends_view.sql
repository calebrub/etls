CREATE MATERIALIZED VIEW IF NOT EXISTS denial_trends_view AS
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
        ltrim(split_part(regexp_replace(dt.charge_rev_code::text, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(dt.charge_cpt_code, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        -- numeric cleaning
        NULLIF(replace(replace(dt.charge_amount::text, '$', ''), ',', ''), '')::numeric AS int_charge_amount,
        NULLIF(replace(replace(dt.insurance_paid_amount::text, '$', ''), ',', ''), '')::numeric AS int_insurance_paid_amount,
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
    ) AS unique_id,
    COALESCE(dtc."group", 'Other') AS denial_group,
    dtc.rcm_section
FROM denial_trends_cte
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = charge_primary_payer_name
LEFT JOIN denial_trends_crosswalk dtc ON dtc.remittance_code = unpaid_reason_code_s;

CREATE INDEX IF NOT EXISTS idx_denial_trends_account ON denial_trends_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_denial_trends_instance_key ON denial_trends_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_denial_trends_received_date ON denial_trends_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_denial_trends_entered_date ON denial_trends_view(charge_entered_date);

REFRESH MATERIALIZED VIEW denial_trends_view;
