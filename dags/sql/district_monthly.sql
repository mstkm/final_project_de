SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month, 
	SUM(ps.closecontact_dikarantina) AS total 
FROM pikobar_staging ps
JOIN dim_case dc
	ON dc.status_detail = 'closecontact_dikarantina'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.closecontact_discarded) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'closecontact_discarded'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.closecontact_meninggal) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'closecontact_meninggal'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.confirmation_meninggal) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'confirmation_meninggal'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.confirmation_sembuh) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'confirmation_sembuh'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.probable_diisolasi) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'probable_diisolasi'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.probable_discarded) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'probable_discarded'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.probable_meninggal) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'probable_meninggal'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.suspect_diisolasi) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'suspect_diisolasi'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.suspect_discarded) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'suspect_discarded'
GROUP BY month, ps.kode_kab, dc.id
UNION ALL
SELECT 
	ps.kode_kab as district_id,
	dc.id as case_id,
	MONTH(tanggal) as month,
	SUM(ps.suspect_meninggal) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'suspect_meninggal'
GROUP BY month, ps.kode_kab, dc.id