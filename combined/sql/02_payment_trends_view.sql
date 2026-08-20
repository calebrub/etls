CREATE MATERIALIZED VIEW IF NOT EXISTS payment_trends_view
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
    NULLIF(replace(replace(pt.insurance_paid_amount::text, '$', ''), ',', ''), '')::numeric AS insurance_paid_amount,
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
        WHEN NULLIF(replace(replace(pt.payment_allowed_amount::text, '$', ''), ',', ''), '')::numeric > 0 THEN 'Paid'
        ELSE 'Not Paid'
    END AS payment_status,
    NULLIF(replace(replace(pt.payment_allowed_amount::text, '$', ''), ',', ''), '')::numeric AS int_payment_allowed_amount,
    coalesce(loc1.level_of_care, loc2.level_of_care) AS level_of_care,
    CASE
        WHEN NULLIF(replace(replace(pt.insurance_paid_amount::text, '$', ''), ',', ''), '')::numeric > 0 THEN true
        ELSE false
    END AS has_insurance_payment,
    NULLIF(replace(replace(pt.insurance_paid_amount::text, '$', ''), ',', ''), '')::numeric AS int_insurance_paid_amount,
    initcap(to_char(pt.payment_received::date::timestamp with time zone, 'day')) AS payment_received_day,
    'Week' || to_char(pt.payment_received::date::timestamp with time zone, 'IW') AS payment_received_week,
    to_char(pt.payment_received::date::timestamp with time zone, 'Month') AS payment_received_month,
    to_char(pt.payment_received::date::timestamp with time zone, 'YYYY') AS payment_received_year,
    pt.payment_entered::date - pt.payment_received::date AS payment_posting_tat,
    NULLIF(replace(replace(pt.patient_applied_amount::text, '$', ''), ',', ''), '')::numeric AS int_payment_applied_amount,
    NULLIF(replace(replace(pt.payment_total_applied::text, '$', ''), ',', ''), '')::numeric AS int_payment_total_applied,
    NULLIF(replace(replace(pt.payment_total_paid::text, '$', ''), ',', ''), '')::numeric AS int_payment_total_paid,
    NULLIF(replace(replace(pt.payment_unapplied_amount::text, '$', ''), ',', ''), '')::numeric AS int_payment_unapplied_amount,
    pnc.payer_code AS payer_class,
    instance_key
FROM payment_trend pt
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = ltrim(split_part(regexp_replace(pt.charge_rev_code::text, '\.0$', ''), ' ', 1), '0')
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = ltrim(split_part(regexp_replace(pt.charge_cpt_code, '\.0$', ''), ' ', 1), '0')
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = pt.charge_primary_payer_name;

CREATE INDEX IF NOT EXISTS idx_payment_trends_account ON payment_trends_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_payment_trends_received_date ON payment_trends_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_payment_trends_entered_date ON payment_trends_view(charge_entered_date);
CREATE INDEX IF NOT EXISTS idx_payment_trends_instance_key ON payment_trends_view(instance_key);

REFRESH MATERIALIZED VIEW payment_trends_view;
