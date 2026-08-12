CREATE MATERIALIZED VIEW IF NOT EXISTS leenxa.insurance_info_view AS
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
  DATE_TRUNC('hour', i."completedAt") AS completed_time_rounded_down,  
  i."processingStartedAt" AS insurance_info_processing_started_at,
  (EXTRACT(EPOCH FROM (i."completedAt" - i."createdAt")) / 3600) AS turn_around_time_hours, 
  i."organizationId" AS ins_info_organisation_id, 
  i."vobStage" AS ins_info_vob_stage,
  COALESCE(lr.client_name, i."sunshineCenter") AS sunshine_center_name,
  NOW() AS data_as_of
  
FROM leenxa."insuranceInfo" AS i
LEFT JOIN leenxa."LocRates" AS lr 
  ON lr.id = (
    CASE 
      WHEN i."sunshineCenter" IS NULL OR i."sunshineCenter" = '' OR i."sunshineCenter" = ' ' THEN NULL
      WHEN i."sunshineCenter" ~ '^\d+$' THEN i."sunshineCenter"::int
      ELSE NULL 
    END
  )
  
WHERE i."id" IS NOT NULL AND i."id" != ''
  AND i."createdBy" IS NOT NULL 
  AND i."createdBy" NOT IN ('', ' ');
