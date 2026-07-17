CREATE MATERIALIZED VIEW IF NOT EXISTS pdr3_calculator_view AS
WITH base AS (
    SELECT
        p.*,

        -- 1. Location Code
        split_part(p.practice_name, ' ', 1) AS location_code,

        -- 2. Clean numeric fields
        NULLIF(replace(replace(p.insurance_paid_amount, '$', ''), ',', ''), '')::numeric AS int_insurance_paid_amount,
        NULLIF(replace(replace(p.patient_paid_amount_w_copays, '$', ''), ',', ''), '')::numeric AS int_patient_paid_amount,
        NULLIF(replace(replace(p.charge_balance_due_ins, '$', ''), ',', ''), '')::numeric AS int_charge_balance_due_ins,
        NULLIF(replace(replace(p.charge_balance_due_pat, '$', ''), ',', ''), '')::numeric AS int_charge_balance_due_pat,
        NULLIF(replace(replace(p.charge_balance_at_collections, '$', ''), ',', ''), '')::numeric AS int_charge_balance_at_collections,

        -- 3. Total Payment Received
        (
            COALESCE(NULLIF(replace(replace(p.insurance_paid_amount, '$', ''), ',', ''), '')::numeric, 0) +
            COALESCE(NULLIF(replace(replace(p.patient_paid_amount_w_copays, '$', ''), ',', ''), '')::numeric, 0)
        ) AS total_payment_received

    FROM pdr3_calculator p

    --
    WHERE
        -- last 12 months based on payment_entered
--        p.payment_entered >= (NOW() AT TIME ZONE 'MST')::date - INTERVAL '12 months'
--         p.payment_entered >= '2026-03-08'::date - INTERVAL '12 months'
        p.payment_entered between '2025-04-01' and '2026-03-31'

        -- exclude null/blank practice name
        AND p.practice_name IS NOT NULL
        AND TRIM(p.practice_name) <> ''

        -- exclude null/blank credit payer
        AND p.credit_payer_name IS NOT NULL
        AND TRIM(p.credit_payer_name) <> ''
),

enriched AS (
    SELECT
        b.*,

        -- 4. Level of Care
        COALESCE(loc1.level_of_care, loc2.level_of_care) AS loc,

        -- 5. Payer Class
        pnc.payer_code AS payer_class,

        -- 6. Status
        CASE
            WHEN b.int_charge_balance_due_ins > 0 THEN 'DO NOT INCLUDE'
            ELSE 'INCLUDE'
        END AS status

    FROM base b
    LEFT JOIN dw_combined.loc_crosswalk loc1
        ON loc1.rev_code::text = b.charge_rev_code::text
    LEFT JOIN dw_combined.loc_crosswalk loc2
        ON loc2.rev_code::text = b.charge_cpt_code
    LEFT JOIN dw_combined.payer_name_crosswalk pnc
        ON pnc.payer_name = b.credit_payer_name
)

SELECT
    customer_account,
    instance_key,
    facility_name,
    practice_name,
    location_code,
    charge_cpt_code,
    charge_rev_code,
    loc,
    charge_amount,
    payment_allowed_amount,
    primary_group_number,
    primary_member_id,
    credit_payer_name,
    payer_class,
    NULLIF(replace(replace(insurance_paid_amount::text, '$', ''), ',', ''), '')::numeric as insurance_paid_amount,
    patient_paid_amount_w_copays,
    total_payment_received,
    payment_received,
    payment_entered,
    charge_from_date,
    charge_to_date,
    primary_ins_zip,
    primary_ins_city,
    primary_ins_state,
    primary_ins_addr_1,
    patient_zip,
    patient_city,
    patient_state,
    patient_address_1,
    charge_balance_due_ins,
    status,
    charge_balance_due_pat,
    charge_balance_at_collections,

    -- Unique ID
    CONCAT(
        REGEXP_REPLACE(location_code, '\s+', '', 'g'),
        REGEXP_REPLACE(payer_class, '\s+', '', 'g'),
        REGEXP_REPLACE(loc, '\s+', '', 'g')
         ) AS unique_id,

    created_at

FROM enriched;

CREATE INDEX IF NOT EXISTS idx_pdr3_calculator_account ON pdr3_calculator_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_pdr3_calculator_entered ON pdr3_calculator_view(payment_entered);
CREATE INDEX IF NOT EXISTS idx_pdr3_calculator_instance_key ON pdr3_calculator_view(instance_key);
-- Index the Unique ID used for Tableau relationships
CREATE INDEX IF NOT EXISTS idx_pdr3_calculator_unique_id ON pdr3_calculator_view(unique_id);

REFRESH MATERIALIZED VIEW pdr3_calculator_view;
