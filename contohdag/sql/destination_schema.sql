-- create dim_province table
CREATE TABLE IF NOT EXISTS dim_province (
    province_id BIGINT PRIMARY KEY,
    province_name VARCHAR(255)
);

-- create dim_district table
CREATE TABLE IF NOT EXISTS dim_district (
    district_id BIGINT PRIMARY KEY,
    province_id BIGINT,
    district_name VARCHAR(255)
);

-- create dim_case table
DROP TYPE IF EXISTS status_enum CASCADE;
CREATE TYPE status_enum AS ENUM ('SUSPECT', 'CLOSECONTACT', 'PROBABLE', 'CONFIRMATION');
DROP TABLE IF EXISTS dim_case;
CREATE TABLE dim_case (
    id SERIAL PRIMARY KEY,
    status_name status_enum,
    status_detail VARCHAR(255)
);
INSERT INTO dim_case (status_name, status_detail)
VALUES ('CLOSECONTACT', 'closecontact_dikarantina'),
('CLOSECONTACT', 'closecontact_discarded'), 
('CLOSECONTACT', 'closecontact_meninggal'),
('CONFIRMATION', 'confirmation_meninggal'), 
('CONFIRMATION', 'confirmation_sembuh'), 
('PROBABLE', 'probable_diisolasi'), 
('PROBABLE', 'probable_discarded'), 
('PROBABLE', 'probable_meninggal'), 
('SUSPECT', 'suspect_diisolasi'), 
('SUSPECT', 'suspect_discarded'), 
('SUSPECT', 'suspect_meninggal');


-- create province_daily table
CREATE TABLE IF NOT EXISTS province_daily (
    id SERIAL PRIMARY KEY,
    province_id BIGINT,
    case_id BIGINT,
    date DATE,
    total INTEGER
);

-- create province_monthly table
CREATE TABLE IF NOT EXISTS province_monthly (
    id SERIAL PRIMARY KEY,
    province_id BIGINT,
    case_id BIGINT,
    month INTEGER,
    total INTEGER
);

-- create province_yearly table
CREATE TABLE IF NOT EXISTS province_yearly (
    id SERIAL PRIMARY KEY,
    province_id BIGINT,
    case_id BIGINT,
    year INTEGER,
    total INTEGER
);

-- create district_monthly table
CREATE TABLE IF NOT EXISTS district_monthly (
    id SERIAL PRIMARY KEY,
    district_id BIGINT,
    case_id BIGINT,
    month INTEGER,
    total INTEGER
);

-- create district_yearly table
CREATE TABLE IF NOT EXISTS district_yearly (
    id SERIAL PRIMARY KEY,
    district_id BIGINT,
    case_id BIGINT,
    year INTEGER,
    total INTEGER
);