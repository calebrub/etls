CREATE OR REPLACE VIEW ar_aging_view AS
WITH ar_aging_cte AS (
    SELECT
        aa.customer_account,
        aa.charge_id,
        aa.claim_id AS claim_id,
        aa.instance_key,
        aa.claim_first_billed_date as claim_first_billed_date,
        aa.charge_from_date,
        aa.charge_to_date,
        aa.charge_entered_date,
        aa.charge_primary_payer_name AS charge_primary_payer_name,
        aa.charge_primary_payer_id,
        aa.practice_name,
        aa.practice_name AS office_name, -- alias for compatibility
        aa.charge_primary_payer_name AS charge_current_payer_name, -- approximate mapping
        aa.practice_name AS facility_name, -- alias for compatibility
        regexp_replace(aa.patient_full_name, '\s*\(\d+\)$', '') AS patient_full_name,
        aa.patient_id AS claim_patient_id,
        aa.charge_cpt_code,
        aa.charge_cpt_description,
        aa.revenue_code AS revenue_code,
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
        -- numeric conversions
        replace(replace(aa.charge_balance_due_ins::text, '$', ''), ',', '')::numeric AS int_charge_balance_due_ins,
        replace(replace(aa.charge_balance_due_other::text, '$', ''), ',', '')::numeric AS int_charge_balance_due_other,
        aa.patient_stmts_sent_electronically::numeric AS int_patient_stmts_sent_electronically,
        aa.patient_statements_printed::numeric AS int_patient_statements_printed,
        replace(replace(aa.charge_balance_due_pat::text, '$', ''), ',', '')::numeric AS int_charge_balance_due_pat
    FROM ar_aging aa
)
SELECT
    ac.customer_account,
    ac.charge_id,
    ac.claim_id,
    ac.claim_first_billed_date,
    ac.charge_from_date,
    ac.charge_to_date,
    ac.charge_entered_date,
    ac.charge_primary_payer_name,
    ac.charge_primary_payer_id,
    ac.practice_name,
    ac.office_name,
    ac.charge_current_payer_name,
    ac.facility_name,
    ac.claim_patient_id,
    ac.charge_cpt_code,
    ac.charge_cpt_description,
    ac.revenue_code,
    ac.patient_full_name,
    ac.charge_fromdate_age,
    ac.charge_fromdate_age_days,
    ac.charge_first_bill_date_age,
    ac.charge_first_bill_date_age_days,
    ac.charge_balance,
    ac.charge_balance_due_ins,
    ac.charge_balance_due_other,
    ac.charge_balance_due_pat,
    ac.charge_balance_at_collections,
    ac.charge_insurance_payments,
    ac.charge_patient_payments,
    ac.charge_total_payments,
    ac.patient_stmts_sent_electronically,
    ac.patient_statements_printed,
    ac.int_charge_balance_due_ins,
    ac.int_charge_balance_due_other,
    ac.int_patient_stmts_sent_electronically,
    ac.int_patient_statements_printed,
    ac.int_charge_balance_due_pat,
    -- calculations
    ac.int_charge_balance_due_ins + ac.int_charge_balance_due_other AS balance_due_payer,
    ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically AS total_statements,
    CASE
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) < 3 THEN 'less than 3 statements'
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) BETWEEN 3 AND 4 THEN '3-4 statements'
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) BETWEEN 5 AND 6 THEN '5-6 statements'
        WHEN (ac.int_patient_statements_printed + ac.int_patient_stmts_sent_electronically) > 6 THEN 'over 6 statements'
        ELSE NULL
    END AS total_statement_buckets,
    pc.payer_code
FROM ar_aging_cte ac
LEFT JOIN payer_name_crosswalk pc
    ON pc.payer_name::text = ac.charge_primary_payer_name;

-----
CREATE OR REPLACE VIEW gross_billing_view AS
WITH gross_billing_cte AS (
    SELECT
        gb.customer_account::varchar AS customer_account,
        gb.practice_name AS office_name,                     -- V2: map practice_name
        gb.practice_name AS facility_name,                   -- V2: map practice_name
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
        gb.charge_primary_payer_name_1 AS charge_primary_payer_name_dup,
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
        lc.level_of_care AS loc,
        replace(replace(gb.charge_amount::text, '$', ''), ',', '')::numeric AS int_charge_amount,
        gb.claim_first_billed_date::date - EXTRACT(dow FROM gb.claim_first_billed_date::date)::integer AS first_billed_week_date,
                gb.instance_key
    FROM gross_billing gb
    LEFT JOIN loc_crosswalk lc ON lc.rev_code::text = gb.charge_rev_code::text
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
    charge_primary_payer_name_dup,
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
    loc,
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
    concat(claim_first_billed_month, '', claim_first_billed_year) AS claim_first_billed_ym,     instance_key
FROM gross_billing_cte;
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
as
SELECT pt.customer_account,
       pt.practice_name as facilty_name,
       pt.practice_name as office_name,
       pt.practice_name,
       pt.charge_entered_date,
       pt.charge_from_date,
       pt.charge_to_date,
       pt.patient_full_name,
--        pt.payment_source,
       'Patient' as payment_source,
       pt.payment_allowed_amount,
       pt.charge_patient_id,
       pt.charge_id as charge_id,
       pt.charge_rev_code as charge_rev_code,
       pt.charge_cpt_code,
       pt.type_of_bill,
       pt.claim_id as charge_claim_id,
       pt.charge_primary_payer_name as payer_name,
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
       pt.payment_total_applied as payment_applied_amount,
       pt.payment_unapplied_amount,
       initcap(to_char(pt.charge_from_date::date::timestamp with time zone, 'day'::text))                     AS charge_from_day,
       'Week'::text || to_char(pt.charge_from_date::date::timestamp with time zone,
                               'IW'::text)                                                                    AS charge_from_week,
       to_char(pt.charge_from_date::date::timestamp with time zone,
               'Month'::text)                                                                                 AS charge_from_month,
       to_char(pt.charge_from_date::date::timestamp with time zone,
               'YYYY'::text)                                                                                  AS charge_from_year,
       CASE
           WHEN replace(replace(pt.payment_allowed_amount::text, '$'::text, ''::text), ','::text, ''::text)::numeric > 0::numeric
               THEN 'Paid'::text
           ELSE 'Not Paid'::text
           END                                                                                                AS payment_status,
       replace(replace(pt.payment_allowed_amount::text, '$'::text, ''::text), ','::text,
               ''::text)::numeric                                                                             AS int_payment_allowed_amount,
       lc.level_of_care,
       CASE
           WHEN replace(replace(pt.insurance_paid_amount::text, '$'::text, ''::text), ','::text, ''::text)::numeric > 0::numeric
               THEN true
           ELSE false
           END                                                                                                AS has_insurance_payment,
       replace(replace(pt.insurance_paid_amount::text, '$'::text, ''::text), ','::text,
               ''::text)::numeric                                                                             AS int_insurance_paid_amount,
       initcap(to_char(pt.payment_received::date::timestamp with time zone,
                       'day'::text))                                                                          AS payment_received_day,
       'Week'::text || to_char(pt.payment_received::date::timestamp with time zone,
                               'IW'::text)                                                                    AS payment_received_week,
       to_char(pt.payment_received::date::timestamp with time zone,
               'Month'::text)                                                                                 AS payment_received_month,
       to_char(pt.payment_received::date::timestamp with time zone,
               'YYYY'::text)                                                                                  AS payment_received_year,
       pt.payment_received::date - pt.payment_entered::date                                                   AS payment_posting_tat,
       replace(replace(pt.patient_applied_amount::text, '$'::text, ''::text), ','::text,
               ''::text)::numeric                                                                             AS int_payment_applied_amount,
       replace(replace(pt.payment_total_applied::text, '$'::text, ''::text), ','::text,
               ''::text)::numeric                                                                             AS int_payment_total_applied,
       replace(replace(pt.payment_total_paid::text, '$'::text, ''::text), ','::text,
               ''::text)::numeric                                                                             AS int_payment_total_paid,
       replace(replace(pt.payment_unapplied_amount::text, '$'::text, ''::text), ','::text,
               ''::text)::numeric                                                                             AS int_payment_unapplied_amount,
    instance_key
FROM payment_trend pt
         LEFT JOIN loc_crosswalk lc ON lc.rev_code::text = pt.charge_rev_code::text;

------
CREATE OR REPLACE VIEW chage_on_hold(facility_name, claim_status, level_of_care, total_amount) as
SELECT practice_name as facility_name,
       claim_status,
       charge_cpt_code                                                                               AS level_of_care,
       sum(replace(replace(charge_amount::text, '$'::text, ''::text), ','::text, ''::text)::numeric) AS total_amount,
       instance_key
FROM charges_on_hold coh
GROUP BY  coh.practice_name, facility_name, claim_status, charge_cpt_code, instance_key;

------
CREATE OR REPLACE VIEW v_charges_on_hold(facility_name, claim_status, level_of_care, total_amount) as
SELECT practice_name as facility_name,
       claim_status,
       charge_cpt_code                                                                               AS level_of_care,
       sum(replace(replace(charge_amount::text, '$'::text, ''::text), ','::text, ''::text)::numeric) AS total_amount,
        instance_key
FROM charges_on_hold coh
GROUP BY  coh.practice_name, facility_name, claim_status, charge_cpt_code, instance_key;

------