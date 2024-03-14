SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date, 
	SUM(ps.closecontact_dikarantina) AS total 
FROM pikobar_staging ps
JOIN dim_case dc
	ON dc.status_detail = 'closecontact_dikarantina'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.closecontact_discarded) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'closecontact_discarded'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.closecontact_meninggal) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'closecontact_meninggal'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.confirmation_meninggal) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'confirmation_meninggal'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.confirmation_sembuh) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'confirmation_sembuh'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.probable_diisolasi) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'probable_diisolasi'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.probable_discarded) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'probable_discarded'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.probable_meninggal) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'probable_meninggal'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.suspect_diisolasi) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'suspect_diisolasi'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.suspect_discarded) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'suspect_discarded'
GROUP BY tanggal, ps.kode_prov, dc.id
UNION ALL
SELECT 
	ps.kode_prov as province_id,
	dc.id as case_id,
	tanggal as date,
	SUM(ps.suspect_meninggal) AS total 
FROM pikobar_staging ps
JOIN dim_case dc 
	ON dc.status_detail = 'suspect_meninggal'
GROUP BY tanggal, ps.kode_prov, dc.id