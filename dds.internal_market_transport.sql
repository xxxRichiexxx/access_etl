INSERT INTO dds.internal_market_transport
(
    id_brand
	,type_ts
	,product
	,"year"
	,vin
	,num_body
	,num_engine
	,num_shassis
	,power
	,volume
	,type_engine
	,wheel
	,code_ts
	,"comment"
	,max_massa
	,min_massa
	,model
	,"class"
	,type_model
	,origin
	,body
	,formula
	,factory
	,description_kind
	,description_type
	,eco_type
	,body_type
	,class_new
	,subclass
)
WITH
sq1 AS (
    SELECT *
    FROM stage.registrations r
    WHERE date_oper BETWEEN '{{ti.xcom_pull(key='min_date', task_ids='Загрузка_данных_в_stage_слой.get_data')}}'
        AND '{{ti.xcom_pull(key='max_date', task_ids='Загрузка_данных_в_stage_слой.get_data')}}'
),
sq2 AS(
    SELECT b.brand, b.country, t.*
    FROM dds.internal_market_brands b
    JOIN dds.internal_market_transport t
        ON t.id_brand = b.id
)
SELECT DISTINCT
    b.id
    ,sq1.type_ts
    ,sq1.product
    ,sq1."year"
    ,sq1.vin
    ,sq1.num_body
    ,sq1.num_engine
    ,sq1.num_shassis
    ,sq1.power
    ,sq1.volume
    ,sq1.type_engine
    ,sq1.wheel
    ,sq1.code_ts
    ,sq1."comment"
    ,sq1.max_massa
    ,sq1.min_massa
    ,sq1.model
    ,sq1."class"
    ,sq1.type_model
    ,sq1.origin
    ,sq1.body
    ,sq1.formula
    ,sq1.factory
    ,sq1.description_kind
    ,sq1.description_type
    ,sq1.eco_type
    ,sq1.body_type
    ,sq1.class_new
    ,sq1.subclass
FROM sq1
LEFT JOIN sq2
    ON  COALESCE(sq1.brand, '0') = COALESCE(sq2.brand, '0')
        AND COALESCE(sq1.country_brand, '0') = COALESCE(sq2.country, '0') 
        AND COALESCE(sq1.type_ts, '0') = COALESCE(sq2.type_ts, '0')
        AND COALESCE(sq1.product, '0') = COALESCE(sq2.product, '0')
        AND COALESCE(sq1."year", '0') = COALESCE(sq2."year", '0')
        AND COALESCE(sq1.vin, '0') = COALESCE(sq2.vin, '0')
        AND COALESCE(sq1.num_body, '0') = COALESCE(sq2.num_body, '0')
        AND COALESCE(sq1.num_engine, '0') = COALESCE(sq2.num_engine, '0')
        AND COALESCE(sq1.num_shassis, '0') = COALESCE(sq2.num_shassis, '0')
        AND COALESCE(sq1.power, '0') = COALESCE(sq2.power, '0')
        AND COALESCE(sq1.volume, '0') = COALESCE(sq2.volume, '0')
        AND COALESCE(sq1.type_engine, '0') = COALESCE(sq2.type_engine, '0')
        AND COALESCE(sq1.wheel, '0') = COALESCE(sq2.wheel, '0')
        AND COALESCE(sq1.code_ts, '0') = COALESCE(sq2.code_ts, '0')
        AND COALESCE(sq1."comment", '0') = COALESCE(sq2."comment", '0')
        AND COALESCE(sq1.max_massa, '0') = COALESCE(sq2.max_massa, '0')
        AND COALESCE(sq1.min_massa, '0') = COALESCE(sq2.min_massa, '0')
        AND COALESCE(sq1.model, '0') = COALESCE(sq2.model, '0')
        AND COALESCE(sq1."class", '0') = COALESCE(sq2."class", '0')
        AND COALESCE(sq1.type_model, '0') = COALESCE(sq2.type_model, '0')
        AND COALESCE(sq1.origin, '0') = COALESCE(sq2.origin, '0')
        AND COALESCE(sq1.body, '0') = COALESCE(sq2.body, '0')
        AND COALESCE(sq1.formula, '0') = COALESCE(sq2.formula, '0')
        AND COALESCE(sq1.factory, '0') = COALESCE(sq2.factory, '0')
        AND COALESCE(sq1.description_kind, '0') = COALESCE(sq2.description_kind, '0')
        AND COALESCE(sq1.description_type, '0') = COALESCE(sq2.description_type, '0')
        AND COALESCE(sq1.eco_type, '0') = COALESCE(sq2.eco_type, '0')
        AND COALESCE(sq1.body_type, '0') = COALESCE(sq2.body_type, '0')
        AND COALESCE(sq1.class_new, '0') = COALESCE(sq2.class_new, '0')
        AND COALESCE(sq1.subclass, '0') = COALESCE(sq2.subclass, '0')
LEFT JOIN dds.internal_market_brands b
    ON COALESCE(sq1.brand, '0') = COALESCE(b.brand, '0')
    AND COALESCE(sq1.country_brand, '0') = COALESCE(b.country, '0')
WHERE sq2.brand IS NULL;