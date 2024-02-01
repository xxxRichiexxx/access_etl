INSERT INTO dds.internal_market_owner
(
    full_name_owner
	,inn
	,district_owner
	,city_owner
	,district_new
	,okved2
	,activity_type
	,coato_owner
	,form_ownership
	,affiliation 
)
SELECT DISTINCT
    r.full_name_owner
	,r.inn
	,r.district_owner
	,r.city_owner
	,r.district_new
	,r.okved2
	,r.activity_type
	,r.coato_owner
	,r.form_ownership
	,r.affiliation 
FROM stage.registrations r
LEFT JOIN dds.internal_market_owner o 
    ON
    COALESCE(o.full_name_owner,'0') = COALESCE(r.full_name_owner, '0')
	AND COALESCE(o.inn,'0') = COALESCE(r.inn, '0')
	AND COALESCE(o.district_owner,'0') = COALESCE(r.district_owner, '0')
	AND COALESCE(o.city_owner,'0') = COALESCE(r.city_owner, '0')
	AND COALESCE(o.district_new,'0') = COALESCE(r.district_new, '0')
	AND COALESCE(o.okved2,'0') = COALESCE(r.okved2, '0')
	AND COALESCE(o.activity_type,'0') = COALESCE(r.activity_type, '0')
	AND COALESCE(o.coato_owner,'0') = COALESCE(r.coato_owner, '0')
	AND COALESCE(o.form_ownership,'0') = COALESCE(r.form_ownership, '0')
	AND COALESCE(o.affiliation,'0') = COALESCE(r.affiliation, '0')
WHERE r.date_oper BETWEEN '{{ti.xcom_pull(key='min_date', task_ids='Загрузка_данных_в_stage_слой.get_data')}}'
    AND '{{ti.xcom_pull(key='max_date', task_ids='Загрузка_данных_в_stage_слой.get_data')}}'
    AND o.full_name_owner IS NULL;