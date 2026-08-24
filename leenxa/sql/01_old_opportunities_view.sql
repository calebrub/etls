DROP MATERIALIZED VIEW IF EXISTS leenxa.old_opportunities_view;

CREATE MATERIALIZED VIEW leenxa.old_opportunities_view AS
SELECT 
  o.id AS opportunity_id,
  o."opportunityName" AS opportunity_name,
  o."vobStatus" AS vob_status,
  COALESCE(NULLIF(TRIM(lr.client_name), ''), NULLIF(TRIM(o."sunshineCenter"), ''), 'Unassigned') AS sunshine_center,
  o."insuranceProvider" AS insurance_provider,
  o."createdBy" AS created_by,
  o."createdAt" AS created_at,
  o."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE 'America/Denver' AS "createAt_mt",
  EXTRACT(DAY FROM o."createdAt") AS created_day,
  TO_CHAR(o."createdAt", 'Day') AS created_day_name2,
  EXTRACT(MONTH FROM o."createdAt") AS created_month,
  EXTRACT(YEAR FROM o."createdAt") AS created_year,
  o."processingStatus" AS processing_status,
  o."stage" AS stage,

  i.id AS insurance_info_id,
  i."createdAt" AS insurance_info_created_at,  
  i."updatedAt" AS insurance_info_updated_at,
  i."verificationStatus" AS insurance_info_verification_status,
  i."completedAt" AS insurance_info_completed_at,
  
  -- Cleaned completedAt
  CASE 
    WHEN i."completedAt" IS NULL THEN NULL
    WHEN i."completedAt" <= '1971-01-01' THEN NULL
    WHEN i."completedAt" < i."createdAt" - INTERVAL '1 minute' THEN NULL
    ELSE GREATEST(i."completedAt", i."createdAt")
  END AS "completeAt_cln",

  DATE_TRUNC('hour', i."completedAt") AS completed_time_rounded_down,  
  i."processingStartedAt" AS insurance_info_processing_started_at,
  
  -- Turn around time in hours using completeAt_cln
  (EXTRACT(EPOCH FROM (
    CASE 
      WHEN i."completedAt" IS NULL THEN NULL
      WHEN i."completedAt" <= '1971-01-01' THEN NULL
      WHEN i."completedAt" < i."createdAt" - INTERVAL '1 minute' THEN NULL
      ELSE GREATEST(i."completedAt", i."createdAt")
    END - i."createdAt"
  )) / 3600) AS turn_around_time_hours,
  
  -- Mountain Time conversion
  (CASE 
    WHEN i."completedAt" IS NULL THEN NULL
    WHEN i."completedAt" <= '1971-01-01' THEN NULL
    WHEN i."completedAt" < i."createdAt" - INTERVAL '1 minute' THEN NULL
    ELSE GREATEST(i."completedAt", i."createdAt")
  END) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Denver' AS "completeAt_cln_mt",
  
  u.name AS user_name
  
FROM leenxa.opportunities AS o
LEFT JOIN leenxa."insuranceInfo" AS i ON o."insuranceInformationId" = i.id
LEFT JOIN leenxa."User" AS u ON o."vobAgentId" = u.id
LEFT JOIN leenxa."LocRates" AS lr 
  ON lr.id = (
    CASE 
      WHEN o."sunshineCenter" IS NULL OR TRIM(o."sunshineCenter") = '' THEN NULL
      WHEN o."sunshineCenter" ~ '^\d+$' THEN o."sunshineCenter"::int
      ELSE NULL 
    END
  );
