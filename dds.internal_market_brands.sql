INSERT INTO dds.internal_market_brands
(brand, country)
SELECT DISTINCT
    r.brand
    ,r.country_brand
FROM stage.registrations r
LEFT JOIN dds.internal_market_brands b
    ON COALESCE(r.brand, '0') = COALESCE(b.brand, '0')
    AND COALESCE(r.country_brand, '0') = COALESCE(b.country, '0')
WHERE b.brand IS NULL
    AND r.date_oper BETWEEN '{{ti.xcom_pull(key='min_date', task_ids='Загрузка_данных_в_stage_слой.get_data')}}'
        AND '{{ti.xcom_pull(key='max_date', task_ids='Загрузка_данных_в_stage_слой.get_data')}}';