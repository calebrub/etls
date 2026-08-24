DROP MATERIALIZED VIEW IF EXISTS leenxa.insurance_info_view;

CREATE MATERIALIZED VIEW leenxa.insurance_info_view AS
SELECT 
  i."id" AS insurance_info_id,
  i."createdAt" AS insurance_info_created_at,  
  i."updatedAt" AS insurance_info_updated_at,
  i."createdBy" AS insurance_info_created_by,
  i."clientName" AS insurance_info_client_name,
  i."subscriberName" AS insurance_info_subscriber_name,
  i."insuranceProvider" AS insurance_provider_1,
  i."sunshineCenter" AS ins_info_sunshine_center,
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
  
  i."organizationId" AS ins_info_organisation_id, 
  i."vobStage" AS ins_info_vob_stage,
  
  -- Cleaned location name (blank/null to Unassigned)
  COALESCE(NULLIF(TRIM(lr.client_name), ''), NULLIF(TRIM(i."sunshineCenter"), ''), 'Unassigned') AS sunshine_center_name,
  
  -- Mountain Time conversions
  i."createdAt" AT TIME ZONE 'UTC' AT TIME ZONE 'America/Denver' AS "createAt_mt",
  (CASE 
    WHEN i."completedAt" IS NULL THEN NULL
    WHEN i."completedAt" <= '1971-01-01' THEN NULL
    WHEN i."completedAt" < i."createdAt" - INTERVAL '1 minute' THEN NULL
    ELSE GREATEST(i."completedAt", i."createdAt")
  END) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Denver' AS "completeAt_cln_mt",
  
  NOW() AS data_as_of
  
FROM leenxa."insuranceInfo" AS i
LEFT JOIN leenxa."LocRates" AS lr 
  ON lr.id = (
    CASE 
      WHEN i."sunshineCenter" IS NULL OR TRIM(i."sunshineCenter") = '' THEN NULL
      WHEN i."sunshineCenter" ~ '^\d+$' THEN i."sunshineCenter"::int
      ELSE NULL 
    END
  )
  
WHERE i."id" IS NOT NULL AND i."id" != ''
  AND i."createdBy" IS NOT NULL 
  AND i."createdBy" NOT IN ('', ' ');
