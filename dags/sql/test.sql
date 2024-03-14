SELECT 
	MONTH(tanggal) as month, 
	kode_kab as district_id, 
	SUM(CLOSECONTACT) as CLOSECONTACT, 
	SUM(CONFIRMATION) as CONFIRMATION,
	SUM(PROBABLE) as PROBABLE,
	SUM(SUSPECT) as SUSPECT,
	SUM(closecontact_dikarantina) as closecontact_dikarantina,
	SUM(closecontact_discarded) as closecontact_discarded,
	SUM(closecontact_meninggal) as closecontact_meninggal,
	SUM(confirmation_meninggal) as confirmation_meninggal,
	SUM(confirmation_sembuh) as confirmation_sembuh,
	SUM(probable_discarded) as probable_discarded,
	SUM(probable_diisolasi) as probable_diisolasi,
	SUM(probable_meninggal) as probable_meninggal,
	SUM(suspect_diisolasi) as suspect_diisolasi,
	SUM(suspect_discarded) as suspect_discarded,
	SUM(suspect_meninggal) as suspect_meninggal
FROM pikobar_staging ps 
GROUP BY month, kode_kab;