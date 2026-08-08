-- Facility Rates DDL & Seed Script
CREATE TABLE IF NOT EXISTS facility_rates (
    id SERIAL PRIMARY KEY,
    instance VARCHAR(255),
    facility VARCHAR(255),
    loc VARCHAR(255),
    payer VARCHAR(255),
    inn_oon VARCHAR(255),
    rate NUMERIC(10, 2) NOT NULL,
    practice_name VARCHAR(255),
    payer_code VARCHAR(255),
    unique_id TEXT,
    CONSTRAINT facility_rates_unique_key UNIQUE (facility, loc, payer, inn_oon)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_facility_rates_unique_key ON facility_rates(facility, loc, payer, inn_oon);

INSERT INTO facility_rates (instance, facility, loc, payer, inn_oon, rate, practice_name, payer_code, unique_id) VALUES
('vantage', 'Amity San Diego', 'PHP', 'OON', 'OON', 3800.00, 'AMITY SAN DIEGO', NULL, 'AMITYPHP'),
('vantage', 'Amity San Diego', 'IOP', 'OON', 'OON', 3200.00, 'AMITY SAN DIEGO', NULL, 'AMITYIOP'),
('vantage', 'Amity San Diego', 'OP', 'OON', 'OON', 2200.00, 'AMITY SAN DIEGO', NULL, 'AMITYOP'),
('vantage', 'Amity Palm Beach', 'DTX', 'OON', 'OON', 5750.00, 'AMITY PALM BEACH LLC', NULL, 'AMITYDTX'),
('vantage', 'Amity Palm Beach', 'RTC', 'OON', 'OON', 5250.00, 'AMITY PALM BEACH LLC', NULL, 'AMITYRTC'),
('vantage', '1 Solution Wellness', 'DTX', 'OON', 'OON', 6775.00, '1 SOLUTION WELLNESS LLC', NULL, '1DTX'),
('vantage', '1 Solution Wellness', 'RTC', 'OON', 'OON', 6275.00, '1 SOLUTION WELLNESS LLC', NULL, '1RTC'),
('vantage', 'Iron Wood Recovery', 'DTX', 'OON', 'OON', 6775.00, 'IRONWOOD RECOVERY LLC', NULL, 'IRONWOODDTX'),
('vantage', 'Iron Wood Recovery', 'RTC', 'OON', 'OON', 6275.00, 'IRONWOOD RECOVERY LLC', NULL, 'IRONWOODRTC'),
('vantage', 'Passage To Recovery', 'DTX', 'OON', 'OON', 8600.00, 'PASSAGE TO RECOVERY', NULL, 'PASSAGEDTX'),
('vantage', 'Passage To Recovery', 'RTC', 'OON', 'OON', 8000.00, 'PASSAGE TO RECOVERY', NULL, 'PASSAGERTC'),
('vantage', 'Bridges of Houston', 'PHP', 'OON', 'OON', 5200.00, NULL, NULL, 'PHP'),
('vantage', 'Bridges of Houston', 'IOP', 'OON', 'OON', 4500.00, NULL, NULL, 'IOP'),
('vantage', 'Bridges of Houston', 'OP', 'OON', 'OON', 3800.00, NULL, NULL, 'OP'),
('vantage', 'Bridges of Houston', 'PHP', 'Compsych', 'INN', 626.00, NULL, NULL, 'PHP'),
('vantage', 'Bridges of Houston', 'IOP', 'Compsych', 'INN', 400.00, NULL, NULL, 'IOP'),
('vantage', 'Bridges of Houston', 'SA PHP', 'Cigna', 'INN', 419.00, NULL, 'CIGNA', 'CIGNASAPHP'),
('vantage', 'Bridges of Houston', 'SA IOP', 'Cigna', 'INN', 255.00, NULL, 'CIGNA', 'CIGNASAIOP'),
('vantage', 'Liah Wellness', 'PHP', 'OON', 'OON', 5200.00, NULL, NULL, 'PHP'),
('vantage', 'Liah Wellness', 'IOP', 'OON', 'OON', 4500.00, NULL, NULL, 'IOP'),
('vantage', 'Liah Wellness', 'OP', 'OON', 'OON', 3800.00, NULL, NULL, 'OP'),
('vantage', 'Liah Wellness', 'PHP', 'Scott and White', 'INN', 170.00, NULL, NULL, 'PHP'),
('vantage', 'Liah Wellness', 'SA PHP', 'Cigna', 'INN', 419.00, NULL, 'CIGNA', 'CIGNASAPHP'),
('vantage', 'Liah Wellness', 'SA IOP', 'Cigna', 'INN', 256.00, NULL, 'CIGNA', 'CIGNASAIOP'),
('vantage', 'Liah Wellness', 'IOP', 'Compsych', 'INN', 475.00, NULL, NULL, 'IOP'),
('enhance', 'VTC', 'PHP', 'OON', 'OON', 7500.00, 'VIRTUAL TREATMENT CENTER, LLC', NULL, 'VIRTUALPHP'),
('enhance', 'VTC', 'IOP', 'OON', 'OON', 6500.00, 'VIRTUAL TREATMENT CENTER, LLC', NULL, 'VIRTUALIOP'),
('enhance', 'VTC', 'OP', 'OON', 'OON', 2495.00, 'VIRTUAL TREATMENT CENTER, LLC', NULL, 'VIRTUALOP'),
('enhance', 'CKFT', 'PHP', 'OON', 'OON', 5593.00, 'CK FAMILY THERAPY', NULL, 'CKPHP'),
('enhance', 'CKFT', 'IOP', 'OON', 'OON', 4193.00, 'CK FAMILY THERAPY', NULL, 'CKIOP'),
('enhance', 'CKFT', 'OP', 'OON', 'OON', 2495.00, 'CK FAMILY THERAPY', NULL, 'CKOP'),
('enhance', 'CKFT', 'SA IOP', 'Cigna', 'INN', 255.00, 'CK FAMILY THERAPY', 'CIGNA', 'CKCIGNASAIOP'),
('enhance', 'CKFT', 'MH IOP', 'Cigna', 'INN', 284.00, 'CK FAMILY THERAPY', 'CIGNA', 'CKCIGNAMHIOP'),
('enhance', 'CKFT', 'PHP', 'Cigna', 'INN', 452.00, 'CK FAMILY THERAPY', 'CIGNA', 'CKCIGNAPHP'),
('enhance', 'EMBRACE', 'DTX', 'OON', 'OON', 7693.00, 'EMBRACE TREATMENT, LLC', NULL, 'EMBRACEDTX'),
('enhance', 'EMBRACE', 'RTC', 'OON', 'OON', 6993.00, 'EMBRACE TREATMENT, LLC', NULL, 'EMBRACERTC'),
('enhance', 'EMBRACE', 'DTX', 'Anthem CA', 'INN', 1100.00, 'EMBRACE TREATMENT, LLC', 'ANTHEM CA', 'EMBRACEANTHEMCADTX'),
('enhance', 'EMBRACE', 'RTC', 'Anthem CA', 'INN', 1000.00, 'EMBRACE TREATMENT, LLC', 'ANTHEM CA', 'EMBRACEANTHEMCARTC'),
('enhance', 'EDEN', 'RTC', 'OON', 'OON', 7693.00, 'EDEN BY ENHANCE', NULL, 'EDENRTC'),
('enhance', 'EDEN', 'RTC', 'Optum/UHC/UMR', 'INN', 900.00, 'EDEN BY ENHANCE', 'OPTUM', 'EDENOPTUMRTC'),
('enhance', 'EDEN', 'RTC', 'Anthem CA', 'INN', 1300.00, 'EDEN BY ENHANCE', 'ANTHEM CA', 'EDENANTHEMCARTC'),
('enhance', 'EHG', 'PHP', 'OON', 'OON', 5593.00, 'ENHANCE HEALTH GROUP, LLC', NULL, 'ENHANCEPHP'),
('enhance', 'EHG', 'IOP', 'OON', 'OON', 4193.00, 'ENHANCE HEALTH GROUP, LLC', NULL, 'ENHANCEIOP'),
('enhance', 'EHG', 'OP', 'OON', 'OON', 2495.00, 'ENHANCE HEALTH GROUP, LLC', NULL, 'ENHANCEOP'),
('enhance', 'EHG', 'PHP', 'Anthem CA', 'INN', 600.00, 'ENHANCE HEALTH GROUP, LLC', 'ANTHEM CA', 'ENHANCEANTHEMCAPHP'),
('enhance', 'EHG', 'IOP', 'Anthem CA', 'INN', 450.00, 'ENHANCE HEALTH GROUP, LLC', 'ANTHEM CA', 'ENHANCEANTHEMCAIOP'),
('enhance', 'EHG', 'PHP', 'Aetna', 'INN', 545.00, 'ENHANCE HEALTH GROUP, LLC', 'AETNA', 'ENHANCEAETNAPHP'),
('enhance', 'EHG', 'IOP', 'Aetna', 'INN', 385.00, 'ENHANCE HEALTH GROUP, LLC', 'AETNA', 'ENHANCEAETNAIOP'),
('enhance', 'EHG', 'PHP', 'CA Blue Shield', 'INN', 459.00, 'ENHANCE HEALTH GROUP, LLC', 'CALIFORNIA BLUE SHIELD', 'ENHANCECALIFORNIABLUESHIELDPHP'),
('enhance', 'EHG', 'IOP', 'CA Blue Shield', 'INN', 326.00, 'ENHANCE HEALTH GROUP, LLC', 'CALIFORNIA BLUE SHIELD', 'ENHANCECALIFORNIABLUESHIELDIOP'),
('enhance', 'EHG', 'PHP', 'Cigna/Evernorth', 'INN', 425.00, 'ENHANCE HEALTH GROUP, LLC', 'CIGNA', 'ENHANCECIGNAPHP'),
('enhance', 'EHG', 'IOP', 'Cigna/Evernorth', 'INN', 265.00, 'ENHANCE HEALTH GROUP, LLC', 'CIGNA', 'ENHANCECIGNAIOP'),
('enhance', 'EHG', 'MH PHP', 'Optum/UHC/UMR', 'INN', 610.00, 'ENHANCE HEALTH GROUP, LLC', 'OPTUM', 'ENHANCEOPTUMMHPHP'),
('enhance', 'EHG', 'MH IOP', 'Optum/UHC/UMR', 'INN', 350.00, 'ENHANCE HEALTH GROUP, LLC', 'OPTUM', 'ENHANCEOPTUMMHIOP'),
('enhance', 'EHG', 'PHP', 'Beacon Health Options', 'INN', 300.00, 'ENHANCE HEALTH GROUP, LLC', 'BEACON ', 'ENHANCEBEACONPHP'),
('enhance', 'EHG', 'IOP', 'Beacon Health Options', 'INN', 200.00, 'ENHANCE HEALTH GROUP, LLC', 'BEACON ', 'ENHANCEBEACONIOP'),
('enhance', 'EHG', 'PHP', 'Magellan', 'INN', 412.00, 'ENHANCE HEALTH GROUP, LLC', 'MAGELLAN', 'ENHANCEMAGELLANPHP'),
('enhance', 'EHG', 'IOP', 'Magellan', 'INN', 232.00, 'ENHANCE HEALTH GROUP, LLC', 'MAGELLAN', 'ENHANCEMAGELLANIOP'),
('enhance', 'EHG', 'PHP', 'MHN/HealthNet', 'INN', 675.00, 'ENHANCE HEALTH GROUP, LLC', 'MHN', 'ENHANCEMHNPHP'),
('enhance', 'EHG', 'IOP', 'MHN/HealthNet', 'INN', 550.00, 'ENHANCE HEALTH GROUP, LLC', 'MHN', 'ENHANCEMHNIOP'),
('enhance', 'EHG', 'PHP', 'HealthSmart', 'INN', 700.00, 'ENHANCE HEALTH GROUP, LLC', NULL, 'ENHANCEPHP'),
('enhance', 'EHG', 'IOP', 'HealthSmart', 'INN', 600.00, 'ENHANCE HEALTH GROUP, LLC', NULL, 'ENHANCEIOP'),
('enhance', 'EHG', 'PHP', 'Multiplan', 'INN', 3915.10, 'ENHANCE HEALTH GROUP, LLC', NULL, 'ENHANCEPHP'),
('enhance', 'EHG', 'IOP', 'Multiplan', 'INN', 2935.10, 'ENHANCE HEALTH GROUP, LLC', NULL, 'ENHANCEIOP'),
('enhance', 'EHG', 'PHP', 'Triwest', 'INN', 4194.75, 'ENHANCE HEALTH GROUP, LLC', NULL, 'ENHANCEPHP'),
('enhance', 'EHG', 'IOP', 'Triwest', 'INN', 3144.75, 'ENHANCE HEALTH GROUP, LLC', NULL, 'ENHANCEIOP'),
('vantage', 'NORTHRIDGE ADDICTION TREATMENT CENTER LLC', 'DTX', 'OON', 'OON', 6500.00, 'NORTHRIDGE ADDICTION TREATMENT CENTER LLC', NULL, 'NORTHRIDGEDTX'),
('vantage', 'NORTHRIDGE ADDICTION TREATMENT CENTER LLC', 'RTC', 'OON', 'OON', 6000.00, 'NORTHRIDGE ADDICTION TREATMENT CENTER LLC', NULL, 'NORTHRIDGERTC'),
('vantage', 'THE TRINITY WELLNESS GROUP LLC', 'PHP', 'OON', 'OON', 5000.00, 'THE TRINITY WELLNESS GROUP LLC', NULL, 'THEPHP'),
('vantage', 'THE TRINITY WELLNESS GROUP LLC', 'IOP', 'OON', 'OON', 4500.00, 'THE TRINITY WELLNESS GROUP LLC', NULL, 'THEIOP'),
('vantage', 'THE TRINITY WELLNESS GROUP LLC', 'OP', 'OON', 'OON', 4000.00, 'THE TRINITY WELLNESS GROUP LLC', NULL, 'THEOP'),
('enhance', 'VTC', 'PHP', 'CA Blue Shield', 'INN', 435.00, 'VIRTUAL TREATMENT CENTER LLC', 'CALIFORNIA BLUE SHIELD', 'VIRTUALCALIFORNIABLUESHIELDPHP'),
('enhance', 'VTC', 'IOP', 'CA Blue Shield', 'INN', 320.00, 'VIRTUAL TREATMENT CENTER LLC', 'CALIFORNIA BLUE SHIELD', 'VIRTUALCALIFORNIABLUESHIELDIOP'),
('enhance', 'EMBRACE', 'RTC', 'CA Blue Shield', 'INN', 725.00, 'EMBRACE TREATMENT, LLC', 'CALIFORNIA BLUE SHIELD', 'EMBRACECALIFORNIABLUESHIELDRTC'),
('enhance', 'EMBRACE', 'DTX', 'Aetna', 'INN', 1100.00, 'EMBRACE TREATMENT, LLC', 'AETNA', 'EMBRACEAETNADTX'),
('enhance', 'EMBRACE', 'RTC', 'Aetna', 'INN', 740.00, 'EMBRACE TREATMENT, LLC', 'AETNA', 'EMBRACEAETNARTC'),
('enhance', 'EDEN', 'RTC', 'Aetna', 'INN', 740.00, 'EDEN BY ENHANCE', 'AETNA', 'EDENAETNARTC'),
('vantage', 'Amity San Diego', 'IOP', 'Multiplan-Negotiating agent', 'INN', 0.00, 'AMITY SAN DIEGO LLC', NULL, 'AMITYIOP'),
('vantage', 'Amity San Diego', 'PHP', 'Multiplan-Negotiating agent', 'INN', 0.00, 'AMITY SAN DIEGO LLC', NULL, 'AMITYPHP'),
('vantage', 'Amity San Diego', 'OP', 'Multiplan-Negotiating agent', 'INN', 0.00, 'AMITY SAN DIEGO LLC', NULL, 'AMITYOP'),
('vantage', '1 Solution Wellness', 'DTX', 'Multiplan-Negotiating agent', 'INN', 0.00, '1 SOLUTION WELLNESS LLC', NULL, '1DTX'),
('vantage', '1 Solution Wellness', 'RTC', 'Multiplan-Negotiating agent', 'INN', 0.00, '1 SOLUTION WELLNESS LLC', NULL, '1RTC'),
('vantage', 'Iron Wood Recovery', 'DTX', 'Multiplan-Negotiating agent', 'INN', 0.00, 'IRON WOOD RECOVERY LLC', NULL, 'IRONDTX'),
('vantage', 'Iron Wood Recovery', 'RTC', 'Multiplan-Negotiating agent', 'INN', 0.00, 'IRON WOOD RECOVERY LLC', NULL, 'IRONRTC'),
('vantage', 'Iron Wood Recovery', 'MH RES', 'Multiplan-Negotiating agent', 'INN', 0.00, 'IRON WOOD RECOVERY LLC', NULL, 'IRONMHRES'),
('vantage', 'Bridges of Houston', 'PHP', 'ComPsych', 'INN', 626.00, NULL, NULL, 'PHP'),
('vantage', 'Bridges of Houston', 'IOP', 'ComPsych', 'INN', 400.00, NULL, NULL, 'IOP'),
('vantage', 'Bridges of Houston', 'PHP', 'Cigna', 'INN', 420.00, 'BRIDGES OF HOUSTON LLC', 'CIGNA', 'BRIDGESCIGNAPHP'),
('vantage', 'Bridges of Houston', 'IOP', 'Cigna', 'INN', 256.00, 'BRIDGES OF HOUSTON LLC', 'CIGNA', 'BRIDGESCIGNAIOP'),
('vantage', 'Liah Wellness', 'PHP', 'Cigna', 'INN', 420.00, 'LIAH WELLNESS LLC', 'CIGNA', 'LIAHCIGNAPHP'),
('vantage', 'Liah Wellness', 'IOP', 'ComPsych', 'INN', 475.00, NULL, NULL, 'IOP'),
('vantage', 'Passage To Recovery', 'HOO19', 'Blue Shield of California', 'INN', 0.00, 'PASSAGE TO RECOVERY LLC', NULL, 'PASSAGEHOO19'),
('vantage', 'Passage To Recovery', 'DTX', 'Multiplan-Negotiating agent', 'INN', 0.00, 'PASSAGE TO RECOVERY LLC', NULL, 'PASSAGEDTX'),
('vantage', 'Passage To Recovery', 'RTC', 'Multiplan-Negotiating agent', 'INN', 0.00, 'PASSAGE TO RECOVERY LLC', NULL, 'PASSAGERTC')
ON CONFLICT (facility, loc, payer, inn_oon) DO UPDATE SET
  rate = EXCLUDED.rate,
  practice_name = EXCLUDED.practice_name,
  payer_code = EXCLUDED.payer_code,
  unique_id = EXCLUDED.unique_id,
  instance = EXCLUDED.instance;
