CREATE MATERIALIZED VIEW rev_rec_payments_view AS
WITH rev_rec_payments_cte AS (
    SELECT
        rrp.customer_account::varchar AS customer_account,
        rrp.instance_key,
        rrp.practice_name AS office_name,
        rrp.practice_name AS facility_name,
        rrp.practice_name,
        rrp.charge_cpt_code,
        rrp.charge_rev_code::text AS charge_rev_code,
        rrp.charge_primary_payer_name,
        rrp.charge_id::varchar AS charge_id,
        rrp.charge_claim_id::varchar AS charge_claim_id,
        rrp.charge_amount,
        rrp.payment_allowed_amount,
        rrp.primary_group_number,
        rrp.primary_member_id,
        rrp.credit_payer_name,
        rrp.payment_total_paid,
        rrp.payment_received,
        rrp.payment_entered,
        rrp.charge_from_date,
        rrp.charge_to_date,
        rrp.created_at,
        split_part(rrp.practice_name, ' ', 1) AS location_code,
        -- cleaned codes for LOC lookup
        ltrim(split_part(regexp_replace(rrp.charge_rev_code::text, '\.0$', ''), ' ', 1), '0') AS clean_rev_code,
        ltrim(split_part(regexp_replace(rrp.charge_cpt_code::text, '\.0$', ''), ' ', 1), '0') AS clean_cpt_code,
        NULLIF(replace(replace(rrp.charge_amount::text, '$', ''), ',', ''), '')::numeric AS int_charge_amount,
        NULLIF(replace(replace(rrp.payment_allowed_amount::text, '$', ''), ',', ''), '')::numeric AS int_payment_allowed_amount,
        NULLIF(replace(replace(rrp.payment_total_paid::text, '$', ''), ',', ''), '')::numeric AS int_payment_total_paid,
        to_char(rrp.payment_received::date::timestamp with time zone, 'day') AS payment_received_day,
        'Week' || to_char(rrp.payment_received::date::timestamp with time zone, 'IW') AS payment_received_week,
        to_char(rrp.payment_received::date::timestamp with time zone, 'Month') AS payment_received_month,
        to_char(rrp.payment_received::date::timestamp with time zone, 'YYYY') AS payment_received_year
    FROM rev_rec_payments rrp
),
enriched AS (
    SELECT
        r.*,
        coalesce(loc1.level_of_care, loc2.level_of_care) AS loc,
        pnc.payer_code AS payer_class
    FROM rev_rec_payments_cte r
    LEFT JOIN loc_crosswalk loc1 ON loc1.rev_code = clean_rev_code
    LEFT JOIN loc_crosswalk loc2 ON loc2.rev_code = clean_cpt_code
    LEFT JOIN payer_name_crosswalk pnc ON pnc.payer_name = r.charge_primary_payer_name
),
unique_enriched AS
         (SELECT *,
                 -- Unique ID
                 CONCAT(
                         REGEXP_REPLACE(location_code, '\s+', '', 'g'),
                         REGEXP_REPLACE(payer_class, '\s+', '', 'g'),
                         REGEXP_REPLACE(loc, '\s+', '', 'g')
                 ) AS unique_id,
                 -- Absolute Unique ID: charge_id + charge_claim_id + practice_name
                 CONCAT(charge_id, charge_claim_id, practice_name) AS abs_unique_id
          FROM enriched)
SELECT *
FROM unique_enriched;

CREATE INDEX IF NOT EXISTS idx_rev_rec_payments_account ON rev_rec_payments_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_rev_rec_payments_instance_key ON rev_rec_payments_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_rev_rec_payments_received_date ON rev_rec_payments_view(payment_received);
CREATE INDEX IF NOT EXISTS idx_rev_rec_payments_unique_id ON rev_rec_payments_view(unique_id);
