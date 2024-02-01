EXPLAIN
INSERT INTO dds.internal_market_brands
(brand, country)
SELECT DISTINCT
    brand
    ,country_brand
FROM stage.registrations;

EXPLAIN
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
SELECT DISTINCT
	b.id
	,r.type_ts
	,r.product
	,r."year"
	,r.vin
	,r.num_body
	,r.num_engine
	,r.num_shassis
	,r.power
	,r.volume
	,r.type_engine
	,r.wheel
	,r.code_ts
	,r."comment"
	,r.max_massa
	,r.min_massa
	,r.model
	,r."class"
	,r.type_model
	,r.origin
	,r.body
	,r.formula
	,r.factory
	,r.description_kind
	,r.description_type
	,r.eco_type
	,r.body_type
	,r.class_new
	,r.subclass
FROM stage.registrations r
LEFT JOIN dds.internal_market_brands b
    ON COALESCE(r.brand, '0') = COALESCE(b.brand, '0')
    AND COALESCE(r.country_brand, '0') = COALESCE(b.country, '0'); 

   
EXPLAIN
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
FROM stage.registrations; 

ALTER TABLE dds.internal_market_registration
DROP CONSTRAINT internal_market_registration_id_transport_fk;

ALTER TABLE dds.internal_market_registration
DROP CONSTRAINT internal_market_registration_id_owner_fk;

EXPLAIN
INSERT INTO dds.internal_market_registration
(
	id_transport,
	id_owner,
	coato_registr,      
	date_reg,      	 	 
	date_oper,     	 	 
	code_tech_oper
)
SELECT
    t.id id_transport
    ,o.id id_owner
    ,r.coato_registr
    ,r.date_reg
    ,r.date_oper
    ,r.code_tech_oper
FROM dds.internal_market_brands b
INNER JOIN dds.internal_market_transport t
	ON t.id_brand = b.id
RIGHT JOIN stage.registrations r 
    ON 
    COALESCE(b.brand,'0') = COALESCE(r.brand,'0')
    AND COALESCE(b.country,'0') = COALESCE(r.country_brand,'0')
	AND COALESCE(t.type_ts,'0') = COALESCE(r.type_ts,'0')
	AND COALESCE(t.product,'0') = COALESCE(r.product,'0')
	AND COALESCE(t."year",'0') = COALESCE(r."year",'0')
	AND COALESCE(t.vin,'0') = COALESCE(r.vin,'0')
	AND COALESCE(t.num_body,'0') = COALESCE(r.num_body,'0')
	AND COALESCE(t.num_engine,'0') = COALESCE(r.num_engine,'0')
	AND COALESCE(t.num_shassis,'0') = COALESCE(r.num_shassis,'0')
	AND COALESCE(t.power,'0') = COALESCE(r.power,'0')
	AND COALESCE(t.volume,'0') = COALESCE(r.volume,'0')
	AND COALESCE(t.type_engine,'0') = COALESCE(r.type_engine,'0')
	AND COALESCE(t.wheel,'0') = COALESCE(r.wheel,'0')
	AND COALESCE(t.code_ts,'0') = COALESCE(r.code_ts,'0')
	AND COALESCE(t."comment",'0') = COALESCE(r."comment",'0')
	AND COALESCE(t.max_massa,'0') = COALESCE(r.max_massa,'0')
	AND COALESCE(t.min_massa,'0') = COALESCE(r.min_massa,'0')
	AND COALESCE(t.model,'0') = COALESCE(r.model,'0')
	AND COALESCE(t."class",'0') = COALESCE(r."class",'0')
	AND COALESCE(t.type_model,'0') = COALESCE(r.type_model,'0')
	AND COALESCE(t.origin,'0') = COALESCE(r.origin,'0')
	AND COALESCE(t.body,'0') = COALESCE(r.body,'0')
	AND COALESCE(t.formula,'0') = COALESCE(r.formula,'0')
	AND COALESCE(t.factory,'0') = COALESCE(r.factory,'0')
	AND COALESCE(t.description_kind,'0') = COALESCE(r.description_kind,'0')
	AND COALESCE(t.description_type,'0') = COALESCE(r.description_type,'0')
	AND COALESCE(t.eco_type,'0') = COALESCE(r.eco_type,'0')
	AND COALESCE(t.body_type,'0') = COALESCE(r.body_type,'0')
	AND COALESCE(t.class_new,'0') = COALESCE(r.class_new,'0')
	AND COALESCE(t.subclass,'0') = COALESCE(r.subclass,'0')
LEFT JOIN  dds.internal_market_owner o
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
	AND COALESCE(o.affiliation,'0') = COALESCE(r.affiliation, '0');

ALTER TABLE dds.internal_market_registration
ADD CONSTRAINT internal_market_registration_id_transport_fk FOREIGN KEY(id_transport) REFERENCES dds.internal_market_transport (id);

ALTER TABLE dds.internal_market_registration
ADD CONSTRAINT internal_market_registration_id_owner_fk FOREIGN KEY(id_owner) REFERENCES dds.internal_market_owner (id);
