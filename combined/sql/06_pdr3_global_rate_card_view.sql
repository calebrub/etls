CREATE MATERIALIZED VIEW IF NOT EXISTS pdr3_global_rate_card_view AS
SELECT
    p.practice_name,
    p.facility_name,
    p.instance_key,
    p.location_code,
    p.loc,
    p.payer_class,
    p.unique_id,
    fr.rate,
    fr.inn_oon,

    -- rate calculation
    ROUND(AVG(p.total_payment_received), 2) AS revenue_recognized,
    (
        CASE
        WHEN ROUND(AVG(p.total_payment_received), 2) > fr.rate OR ROUND(AVG(p.total_payment_received), 2) < 0.4 * fr.rate
        THEN 'Needs Review'
        ELSE 'OK'
        END
    ) AS flaged,

    -- optional: label the period (recommended)
    CONCAT('Q', EXTRACT(QUARTER FROM ('2026-03-31'::date)), ' ', EXTRACT(YEAR FROM ('2026-03-31'::date))) AS rate_period,

    NOW() AT TIME ZONE 'MST' AS rate_calculated_at

FROM pdr3_calculator_view p
JOIN facility_rates fr ON
            p.unique_id = fr.unique_id
-- only include usable rows (important)
WHERE p.status = 'INCLUDE' AND upper(p.charge_rev_code) NOT IN ('INT', 'INTEREST',  'INTCHRG')

GROUP BY
    p.practice_name,
    p.instance_key,
    p.location_code,
    p.loc,
    p.payer_class,
    p.unique_id, p.facility_name, fr.inn_oon, fr.rate;

CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_unique_id ON pdr3_global_rate_card_view(unique_id);
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_practice ON pdr3_global_rate_card_view(practice_name) ;
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_location ON pdr3_global_rate_card_view(location_code);
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_payer_class ON pdr3_global_rate_card_view(payer_class);
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_loc ON pdr3_global_rate_card_view(loc);
CREATE INDEX IF NOT EXISTS idx_pdr3_global_rate_instance_key ON pdr3_global_rate_card_view(instance_key);

REFRESH MATERIALIZED VIEW pdr3_global_rate_card_view;
