CREATE MATERIALIZED VIEW IF NOT EXISTS user_time_spread_view AS
SELECT
    uts.customer_account::varchar AS customer_account,
    uts.instance_key,
    uts.practice_name,
    uts.facility_name,
    uts.office_name,
    uts.audit_username,
    uts.audit_action,
    uts.audit_type,
    uts.audit_entered_date,
    uts.audit_entity_id,
    uts.audit_patient_id::varchar AS audit_patient_id,
    uts.patient_full_name,
    uts.created_at
FROM user_time_spread uts;

CREATE INDEX IF NOT EXISTS idx_user_time_spread_account ON user_time_spread_view(customer_account);
CREATE INDEX IF NOT EXISTS idx_user_time_spread_instance_key ON user_time_spread_view(instance_key);
CREATE INDEX IF NOT EXISTS idx_user_time_spread_username ON user_time_spread_view(audit_username);

REFRESH MATERIALIZED VIEW user_time_spread_view;
