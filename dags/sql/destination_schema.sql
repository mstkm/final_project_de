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