CREATE MATERIALIZED VIEW rev_rec_charges_view AS
WITH base AS (
    SELECT
        rrc.customer_account::varchar                                                                  AS customer_account,
        rrc.instance_key,
        rrc.practice_name                                                                             AS office_name,
        rrc.practice_name                                                                             AS facility_name,
        rrc.practice_name,
        rrc.charge_primary_payer_name,
        rrc.charge_primary_payer_id::varchar                                                          AS charge_primary_payer_id,
        rrc.claim_primary_payer_type,
        rrc.charge_secondary_payer_name,
        rrc.claim_type,
        rrc.charge_patient_id::varchar                                                                AS charge_patient_id,
        rrc.charge_id::varchar                                                                        AS charge_id,
        rrc.claim_id::varchar                                                                         AS claim_id,
        rrc.patient_full_name,
        rrc.primary_payer_member_id,
        rrc.charge_from_date,
        rrc.charge_to_date,
        rrc.claim_first_billed_date,
        rrc.times_billed,
        rrc.admission_date,
        rrc.claim_admit_code,
        rrc.charge_cpt_code,
        rrc.charge_rev_code,
        rrc.charge_units_sum::varchar                                                                 AS charge_units_sum,
        rrc.type_of_bill::varchar                                                                     AS type_of_bill,
        rrc.claim_status,
        rrc.charge_amount,
        rrc.primary_group,
        rrc.claim_primary_member_id,
        split_part(rrc.practice_name, ' ', 1)                                                        AS location_code,
        rrc.created_at,
        ltrim(split_part(regexp_replace(rrc.charge_rev_code::text, '\.0$', ''), ' ', 1), '0')        AS clean_rev_code,
        ltrim(split_part(regexp_replace(rrc.charge_cpt_code::text, '\.0$', ''), ' ', 1), '0')        AS clean_cpt_code,
        NULLIF(replace(replace(rrc.charge_amount::text, '$', ''), ',', ''), '')::numeric                         AS int_charge_amount,
        to_char(rrc.claim_first_billed_date::date::timestamptz, 'day')                               AS claim_first_billed_day,
        'Week' || to_char(rrc.claim_first_billed_date::date::timestamptz, 'IW')                      AS claim_first_billed_week,
        to_char(rrc.claim_first_billed_date::date::timestamptz, 'Month')                             AS claim_first_billed_month,
        to_char(rrc.claim_first_billed_date::date::timestamptz, 'YYYY')                              AS claim_first_billed_year
    FROM rev_rec_charges rrc
),
enriched AS (
    SELECT
        b.*,
        coalesce(loc1.level_of_care, loc2.level_of_care)                                             AS loc,
        pnc.payer_code                                                                               AS payer_class,
        CONCAT(
            regexp_replace(split_part(b.practice_name, ' ', 1), '\s+', '', 'g'),
            regexp_replace(pnc.payer_code,                                        '\s+', '', 'g'),
            regexp_replace(coalesce(loc1.level_of_care, loc2.level_of_care),     '\s+', '', 'g')
        )                                                                                            AS unique_id,
        CONCAT(b.charge_id, b.claim_id, b.practice_name)                                            AS abs_unique_id
    FROM base b
    LEFT JOIN loc_crosswalk        loc1 ON loc1.rev_code  = b.clean_rev_code
    LEFT JOIN loc_crosswalk        loc2 ON loc2.rev_code  = b.clean_cpt_code
    LEFT JOIN payer_name_crosswalk pnc  ON pnc.payer_name = b.charge_primary_payer_name
)
SELECT
    e.*,
    fr.rate,
    fr.inn_oon,
    p3.revenue_recognized
FROM enriched e
LEFT JOIN facility_rates fr
    ON e.unique_id = fr.unique_id
LEFT JOIN pdr3_global_rate_card_view p3 ON p3.unique_id = e.unique_id;

-- Index on the materialized view for fast lookups
CREATE INDEX ON rev_rec_charges_view (unique_id);
CREATE INDEX ON rev_rec_charges_view (abs_unique_id);
