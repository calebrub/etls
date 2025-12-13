COPY ar_aging FROM 'ar_aging.dat' WITH (FORMAT text, DELIMITER E'|', NULL '');
COPY charges_on_hold FROM 'charges_on_hold.dat' WITH (FORMAT text, DELIMITER E'|', NULL '');
COPY claim_stage_breakdown FROM 'claim_stage_breakdown.dat' WITH (FORMAT text, DELIMITER E'|', NULL '');
COPY denial_trends FROM 'denial_trends.dat' WITH (FORMAT text, DELIMITER E'|', NULL '');
COPY gross_billing FROM 'gross_billing.dat' WITH (FORMAT text, DELIMITER E'|', NULL '');
COPY payment_trend FROM 'payment_trend.dat' WITH (FORMAT text, DELIMITER E'|', NULL '');
COPY quadrant_performance FROM 'quadrant_performance.dat' WITH (FORMAT text, DELIMITER E'|', NULL '');
COPY rcm_productivity FROM 'rcm_productivity.dat' WITH (FORMAT text, DELIMITER E'|', NULL '');