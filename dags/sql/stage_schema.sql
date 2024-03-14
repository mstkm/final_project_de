CREATE TABLE IF NOT EXISTS pikobar_staging (
    id BIGINT NOT NULL AUTO_INCREMENT,
    tanggal DATE,
    kode_prov BIGINT,
    nama_prov VARCHAR(255),
    kode_kab BIGINT,
    nama_kab VARCHAR(255),
    suspect_diisolasi INT,
    suspect_discarded INT,
    SUSPECT INT,
    closecontact_dikarantina INT,
    closecontact_discarded INT,
    CLOSECONTACT INT,
    probable_diisolasi INT,
    probable_discarded INT,
    PROBABLE INT,
    CONFIRMATION INT,
    confirmation_selesai INT,
    confirmation_meninggal INT,
    suspect_meninggal INT,
    closecontact_meninggal INT,
    probable_meninggal INT,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS dim_province (
    province_id BIGINT,
    province_name VARCHAR(255),
    PRIMARY KEY (province_id)
);

CREATE TABLE IF NOT EXISTS dim_district (
    district_id BIGINT,
    province_id BIGINT,
    district_name VARCHAR(255),
    PRIMARY KEY (district_id)
);

DROP TABLE IF EXISTS dim_case;
CREATE TABLE dim_case (
    id BIGINT NOT NULL AUTO_INCREMENT,
    status_name ENUM('SUSPECT', 'CLOSECONTACT', 'PROBABLE', 'CONFIRMATION'),
    status_detail VARCHAR(255),
    PRIMARY KEY (id)
);
