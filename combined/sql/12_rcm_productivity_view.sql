DROP MATERIALIZED VIEW IF EXISTS rcm_productivity_view;
CREATE MATERIALIZED VIEW rcm_productivity_view AS
WITH rcm_prod_cte AS (
    SELECT
        rp.customer_account::varchar AS customer_account,
        rp.instance_key,
        rp.created_by_user,
        rp.practice_name,
        rp.office_name,
        rp.note_count_cntunq,
        rp.claim_id::varchar AS claim_id,
        rp.claim_first_billed_date,
        rp.claim_last_billed_date,
        rp.last_note_date_max,
        rp.claim_from_date,
        rp.claim_to_date,
        rp.claim_status,
        rp.created_at,
        -- Productivity Date Dimensions (Billing)
        initcap(to_char(rp.claim_first_billed_date::date::timestamp with time zone, 'day')) AS billed_day,
        'Week' || to_char(rp.claim_first_billed_date::date::timestamp with time zone, 'IW') AS billed_week,
        to_char(rp.claim_first_billed_date::date::timestamp with time zone, 'Month') AS billed_month,
        to_char(rp.claim_first_billed_date::date::timestamp with time zone, 'YYYY') AS billed_year,
        -- Productivity Date Dimensions (Notes)
        initcap(to_char(rp.last_note_date_max::date::timestamp with time zone, 'day')) AS last_note_day,
        'Week' || to_char(rp.last_note_date_max::date::timestamp with time zone, 'IW') AS last_note_week,
        to_char(rp.last_note_date_max::date::timestamp with time zone, 'Month') AS last_note_month,
        to_char(rp.last_note_date_max::date::timestamp with time zone, 'YYYY') AS last_note_year,
        -- Lag Calculations
        (NOW() AT TIME ZONE 'MST')::date - rp.last_note_date_max::date AS days_since_last_action,
        rp.last_note_date_max::date - rp.claim_first_billed_date::date AS billing_to_note_lag_days
    FROM rcm_productivity rp
)
SELECT
    *,
    CASE
        WHEN billing_to_note_lag_days < 0 THEN 'Action Before Billing'
        WHEN billing_to_note_lag_days <= 7 THEN '0-7 Days'
        WHEN billing_to_note_lag_days <= 14 THEN '8-14 Days'
        WHEN billing_to_note_lag_days <= 30 THEN '15-30 Days'
        WHEN billing_to_note_lag_days > 30 THEN 'Over 30 Days'
        ELSE 'No Action Recorded'
    END AS follow_up_efficiency_bucket,
    CASE
        WHEN note_count_cntunq = 0 THEN 'Unworked'
        WHEN note_count_cntunq = 1 THEN 'Single Touch'
        WHEN note_count_cntunq BETWEEN 2 AND 5 THEN '2-5 Touches'
        WHEN note_count_cntunq > 5 THEN 'High Touch'
        ELSE 'Unknown'
    END AS touch_point_category
FROM rcm_prod_cte;

CREATE INDEX IF NOT EXISTS idx_rcm_prod_account ON rcm_productivity_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_instance_key ON rcm_productivity_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_user ON rcm_productivity_view(created_by_user);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_claim_id ON rcm_productivity_view(claim_id);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_billed_date ON rcm_productivity_view(claim_first_billed_date);
CREATE INDEX IF NOT EXISTS idx_rcm_prod_note_date ON rcm_productivity_view(last_note_date_max);
