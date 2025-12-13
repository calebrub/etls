-- Create the loc_crosswalk table
CREATE TABLE loc_crosswalk (
    rev_code VARCHAR(20) PRIMARY KEY,
    level_of_care VARCHAR(10)
);

-- Insert the values
INSERT INTO loc_crosswalk (rev_code, level_of_care) VALUES
('126', 'DTX'),
('1002', 'RTC'),
('913', 'PHP'),
('906', 'IOP'),
('905', 'IOP'),
('912', 'PHP'),
('915', 'OP'),
--('128', 'RTC'),
('128', 'Dtx'), 
('1001', 'RTC'),
('H0010', 'DTX'),
('156', 'DTX'),
('H0018', 'RTC'),
('S0201', 'PHP'),
('H0015', 'IOP'),
('S9480', 'IOP'),
('H0019', 'RTC'),
('H0035', 'PHP'),
('H2036', 'PHP'),
('90853', 'OP'),
('H0011', 'DTX'),
('S9475P', 'PHP'),
('S9475', 'IOP'),
('H2013', 'IOP'),
('H0005', 'OP');
---

INSERT INTO loc_crosswalk (rev_code, level_of_care) VALUES
('0126', 'DTX'),
('0906', 'IOP'),
('0156', 'DTX'),
('0913', 'PHP');
