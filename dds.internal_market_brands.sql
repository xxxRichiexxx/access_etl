INSERT INTO dds.internal_market_brands
(brand, country)
WITH
sq AS(
    SELECT DISTINCT
        brand
        ,country_brand
    FROM stage.registrations
    WHERE date_oper BETWEEN '{{ti.xcom_pull(key='min_date', task_ids='Загрузка_данных_в_stage_слой.get_data')}}'
        AND '{{ti.xcom_pull(key='max_date', task_ids='Загрузка_данных_в_stage_слой.get_data')}}'
)
SELECT
    sq.brand
    ,sq.country_brand
FROM sq
LEFT JOIN dds.internal_market_brands b
    ON COALESCE(sq.brand, '0') = COALESCE(b.brand, '0')
    AND COALESCE(sq.country_brand, '0') = COALESCE(b.country, '0')
WHERE b.brand IS NULL
RETURNING COUNT(*);