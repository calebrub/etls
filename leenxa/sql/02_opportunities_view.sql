CREATE MATERIALIZED VIEW IF NOT EXISTS leenxa.opportunities_view AS
SELECT 
  o.id AS opportunity_id,
  o."opportunityName" AS opportunity_name,
  o."vobStatus" AS vob_status,
  COALESCE(lr.client_name, o."sunshineCenter") AS sunshine_center,
  o."insuranceProvider" AS insurance_provider,
  o."createdBy" AS created_by,
  o."createdAt" AS created_at,
  EXTRACT(DAY FROM o."createdAt") AS created_day,
  TO_CHAR(o."createdAt", 'Day') AS created_day_name2,
  EXTRACT(MONTH FROM o."createdAt") AS created_month,
  EXTRACT(YEAR FROM o."createdAt") AS created_year,
  o."processingStatus" AS processing_status,
  o."stage" AS stage,

  i."insuranceProvider" AS insurance_provider_1,
  i."clientName" AS client_name,

  u.name AS user_name
  
FROM leenxa.opportunities AS o
LEFT JOIN leenxa."insuranceInfo" AS i ON o."insuranceInformationId" = i.id
LEFT JOIN leenxa."User" AS u ON o."vobAgentId" = u.id
LEFT JOIN leenxa."LocRates" AS lr ON o."sunshineCenter" ~ '^\d+$' AND lr.id = o."sunshineCenter"::int;
