DROP MATERIALIZED VIEW IF EXISTS leenxa.opportunities_view;

CREATE MATERIALIZED VIEW leenxa.opportunities_view AS
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

  u.name AS user_name
  
FROM leenxa.opportunities AS o
LEFT JOIN leenxa."User" AS u ON o."vobAgentId" = u.id
LEFT JOIN leenxa."LocRates" AS lr 
  ON lr.id = (
    CASE 
      WHEN o."sunshineCenter" IS NULL OR TRIM(o."sunshineCenter") = '' THEN NULL
      WHEN o."sunshineCenter" ~ '^\d+$' THEN o."sunshineCenter"::int
      ELSE NULL 
    END
  );
