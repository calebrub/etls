CREATE MATERIALIZED VIEW IF NOT EXISTS write_off_trend_view AS
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
        wot.adjustment_code,
        wot.adj_code_description,
        wot.charge_cpt_code,
        wot.charge_amount,
        wot.credit_applied,
        wot.insurance_adjustment_amount,
        wot.credit_amount,
        -- numeric cleaning for currency
        NULLIF(replace(replace(wot.charge_amount, '$', ''), ',', ''), '')::numeric AS int_charge_amount,
        NULLIF(replace(replace(wot.credit_applied, '$', ''), ',', ''), '')::numeric AS int_credits_applied,
        NULLIF(replace(replace(wot.insurance_adjustment_amount, '$', ''), ',', ''), '')::numeric AS int_insurance_adjustment_amount,
        NULLIF(replace(replace(wot.credit_amount, '$', ''), ',', ''), '')::numeric AS int_credit_amount,
        NULLIF(replace(replace(wot.patient_total_credits, '$', ''), ',', ''), '')::numeric AS int_patient_total_credits,
        NULLIF(replace(replace(wot.payment_total_applied, '$', ''), ',', ''), '')::numeric AS int_payment_total_applied,
        NULLIF(replace(replace(wot.patient_ins_credits, '$', ''), ',', ''), '')::numeric AS int_patient_ins_credits,
        NULLIF(replace(replace(wot.patient_credits, '$', ''), ',', ''), '')::numeric AS int_patient_credits,
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
    END AS days_on_hold_range,
    COALESCE(wotc.adjustment_type, 'UNCATEGORIZED ADJUSTMENT') AS adjustment_type
FROM wot_cte wc
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = wc.credit_payer_name
LEFT JOIN (
    SELECT DISTINCT ON (adjustment_code)
        adjustment_code,
        adjustment_type
    FROM write_off_trends_crosswalk
) wotc ON wotc.adjustment_code = COALESCE(NULLIF(wc.adjustment_code, ''), 'Null');

CREATE INDEX IF NOT EXISTS idx_write_off_trend_account ON write_off_trend_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_write_off_trend_instance_key ON write_off_trend_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_write_off_trend_received_date ON write_off_trend_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_write_off_trend_entered_date ON write_off_trend_view(charge_entered_date);

REFRESH MATERIALIZED VIEW write_off_trend_view;