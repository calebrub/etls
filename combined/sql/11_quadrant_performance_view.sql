CREATE MATERIALIZED VIEW IF NOT EXISTS quadrant_performance_view AS
WITH qp_cte AS (
    SELECT
        qp.customer_account::varchar AS customer_account,
        qp.instance_key,
        qp.facility_name,
        qp.office_name,
        qp.practice_name,
        qp.charge_entered_date,
        qp.charge_from_date,
        qp.charge_to_date,
        qp.patient_full_name,
        qp.payment_source,
        qp.charge_patient_id::bigint::varchar AS charge_patient_id,
        qp.charge_id::bigint::varchar AS charge_id,
        qp.charge_claim_id::bigint::varchar AS charge_claim_id,
        qp.payer_name,
        qp.primary_payer_member_id,
        qp.payment_received,
        qp.payment_entered,
        qp.primary_ins_zip,
        qp.primary_ins_city,
        qp.primary_ins_state,
        qp.primary_ins_addr_1,
        qp.patient_zip,
        qp.patient_city,
        qp.patient_state,
        qp.patient_address_1,
        qp.created_at,
        qp.type_of_bill::varchar AS type_of_bill,
        qp.charge_cpt_code,
        -- cleaned codes for LOC lookup
        ltrim(split_part(regexp_replace(qp.charge_rev_code::text, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(qp.charge_cpt_code, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        -- numeric cleaning
        NULLIF(replace(replace(qp.payment_allowed_amount, '$', ''), ',', ''), '')::numeric AS int_payment_allowed_amount,
        NULLIF(replace(replace(qp.charge_amount, '$', ''), ',', ''), '')::numeric AS int_charge_amount,
        NULLIF(replace(replace(qp.insurance_paid_amount, '$', ''), ',', ''), '')::numeric AS int_insurance_paid_amount,
        NULLIF(replace(replace(qp.payment_total_paid, '$', ''), ',', ''), '')::numeric AS int_payment_total_paid,
        NULLIF(replace(replace(qp.payment_total_applied, '$', ''), ',', ''), '')::numeric AS int_payment_total_applied,
        NULLIF(replace(replace(qp.charge_insurance_adjustments, '$', ''), ',', ''), '')::numeric AS int_charge_insurance_adjustments,
        NULLIF(replace(replace(qp.charge_patient_adjustments, '$', ''), ',', ''), '')::numeric AS int_charge_patient_adjustments,
        NULLIF(replace(replace(qp.charge_total_adjustments, '$', ''), ',', ''), '')::numeric AS int_charge_total_adjustments,
        NULLIF(replace(replace(qp.insurance_applied_amount, '$', ''), ',', ''), '')::numeric AS int_insurance_applied_amount,
        NULLIF(replace(replace(qp.patient_applied_amount, '$', ''), ',', ''), '')::numeric AS int_patient_applied_amount,
        NULLIF(replace(replace(qp.payment_applied_amount, '$', ''), ',', ''), '')::numeric AS int_payment_applied_amount,
        NULLIF(replace(replace(qp.payment_unapplied_amount, '$', ''), ',', ''), '')::numeric AS int_payment_unapplied_amount,
        -- date dimensions
        (NOW() AT TIME ZONE 'MST')::date - qp.charge_entered_date::date AS days_on_hold,
        to_char(qp.payment_received::date::timestamp with time zone, 'day') AS payment_received_day,
        'Week' || to_char(qp.payment_received::date::timestamp with time zone, 'IW') AS payment_received_week,
        to_char(qp.payment_received::date::timestamp with time zone, 'Month') AS payment_received_month,
        to_char(qp.payment_received::date::timestamp with time zone, 'YYYY') AS payment_received_year
    FROM quadrant_performance qp
)
SELECT
    qp_cte.customer_account,
    qp_cte.instance_key,
    qp_cte.facility_name,
    qp_cte.office_name,
    qp_cte.practice_name,
    qp_cte.charge_entered_date,
    qp_cte.charge_from_date,
    qp_cte.charge_to_date,
    qp_cte.patient_full_name,
    qp_cte.payment_source,
    qp_cte.charge_patient_id,
    qp_cte.charge_id,
    qp_cte.charge_claim_id,
    qp_cte.payer_name,
    qp_cte.primary_payer_member_id,
    qp_cte.payment_received,
    qp_cte.payment_entered,
    qp_cte.primary_ins_zip,
    qp_cte.primary_ins_city,
    qp_cte.primary_ins_state,
    qp_cte.primary_ins_addr_1,
    qp_cte.patient_zip,
    qp_cte.patient_city,
    qp_cte.patient_state,
    qp_cte.patient_address_1,
    qp_cte.created_at,
    qp_cte.type_of_bill,
    qp_cte.int_payment_allowed_amount,
    qp_cte.int_charge_amount,
    qp_cte.int_insurance_paid_amount,
    qp_cte.int_payment_total_paid,
    qp_cte.int_payment_total_applied,
    qp_cte.int_charge_insurance_adjustments,
    qp_cte.int_charge_patient_adjustments,
    qp_cte.int_charge_total_adjustments,
    qp_cte.int_insurance_applied_amount,
    qp_cte.int_patient_applied_amount,
    qp_cte.int_payment_applied_amount,
    qp_cte.int_payment_unapplied_amount,
    qp_cte.days_on_hold,
    qp_cte.payment_received_day,
    qp_cte.payment_received_week,
    qp_cte.payment_received_month,
    qp_cte.payment_received_year,
    qp_cte.charge_cpt_code,
    coalesce(loc1.level_of_care, loc2.level_of_care) AS loc,
    pnc.payer_code AS payer_class,
    -- Unique ID consistent with rate card views
    CONCAT(
        REGEXP_REPLACE(split_part(qp_cte.practice_name, ' ', 1), '\s+', '', 'g'),
        REGEXP_REPLACE(pnc.payer_code, '\s+', '', 'g'),
        REGEXP_REPLACE(coalesce(loc1.level_of_care, loc2.level_of_care), '\s+', '', 'g')
    ) AS unique_id,
    COALESCE(fr.inn_oon, 'OON') AS inn_oon,
    COALESCE(fr.inn_oon, 'OON') AS network_status
FROM qp_cte
LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = qp_cte.clean_rev_code
LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = qp_cte.clean_cpt_code
LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = qp_cte.payer_name
LEFT JOIN facility_rates fr ON fr.unique_id = CONCAT(
    REGEXP_REPLACE(split_part(qp_cte.practice_name, ' ', 1), '\s+', '', 'g'),
    REGEXP_REPLACE(pnc.payer_code, '\s+', '', 'g'),
    REGEXP_REPLACE(coalesce(loc1.level_of_care, loc2.level_of_care), '\s+', '', 'g')
);

CREATE INDEX IF NOT EXISTS idx_quadrant_perf_account ON quadrant_performance_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_quadrant_perf_instance_key ON quadrant_performance_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_quadrant_perf_received_date ON quadrant_performance_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_quadrant_perf_unique_id ON quadrant_performance_view(unique_id);

REFRESH MATERIALIZED VIEW quadrant_performance_view;

