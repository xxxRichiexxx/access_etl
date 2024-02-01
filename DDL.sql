-------------------STAGE-------------------------

DROP TABLE IF EXISTS stage.registrations;
CREATE TABLE stage.registrations (
	id serial NOT NULL,
	coato_registr varchar NULL,      --СОАТО
	type_ts varchar NULL,    	 --Тип т/с
	date_reg date NULL,      	 --Дата регистрации
	date_oper date NULL,     	 --Дата операции
	product varchar NULL,    	 --Товар
	"year" varchar NULL,             --Год
	vin varchar NULL,            --VIN
	num_body varchar NULL,       --N куз
	num_engine varchar NULL,     --N двиг
	num_shassis varchar NULL,    --N шасси
	code_tech_oper varchar NULL, 	 --Первичность
	affiliation varchar NULL,    	 --Принадлежность
	power varchar NULL,            --Мощность
	volume varchar NULL,             --Объем
	type_engine varchar NULL,        --Тип двигателя
	wheel varchar NULL,          --Правосторонность руля
	code_ts varchar NULL,            --Код т/с
	"comment" text NULL,         --Комментарий к коду тс
	max_massa varchar NULL,          --Максимальная масса
	min_massa varchar NULL,          --Масса без нагрузки
	full_name_owner varchar NULL,--Наименование владельца
	inn varchar NULL,                  --ИНН
	district_owner varchar NULL,       --Район
	city_owner varchar NULL,           --Город
	brand varchar NULL,                --Производитель
	model varchar NULL,                --Модель
	"class" varchar NULL,              --Класс
	type_model varchar NULL,       	   --Тип
	origin varchar NULL,           	   --Происхождение
	body varchar NULL,                 --Кузов
	formula varchar NULL,              --Кол фор-ла
	id1 varchar NULL,                  	   --ID1
	district_new varchar NULL,     	   --Район новый
	factory varchar NULL,              --Надстройщик
	description_kind text NULL,        --Описание(вид)
	description_type text NULL,        --Описание(тип)
	country_brand varchar NULL,        --Страна
	eco_type varchar NULL,             --Экология
	body_type varchar NULL,            --Тип кузова
	okved2 varchar NULL,               --Код ОКВЭД2
	activity_type varchar NULL,        --Вид деятельности по ОКВЭД2
	class_new varchar NULL,            --Класс новый
	subclass varchar NULL,             --Подкласс
	coato_owner varchar NULL,              --СОАТО владельца
	form_ownership varchar NULL,       --Форма собственности
	ts TIMESTAMP DEFAULT NOW(),
	
	UNIQUE(id, date_oper)
)
DISTRIBUTED BY(id)
PARTITION BY RANGE(date_oper)
(
	PARTITION P_01_2015 START('2015-01-01') INCLUSIVE END('2015-02-01') EXCLUSIVE
);

-- Table comment

COMMENT ON TABLE stage.registrations IS 'Регистрация транспортных средств';

-- Column comments

COMMENT ON COLUMN stage.registrations.coato_registr IS 'СОАТО места регистрации';
COMMENT ON COLUMN stage.registrations.type_ts IS 'Тип ТС';
COMMENT ON COLUMN stage.registrations.date_reg IS 'Дата регистрации';
COMMENT ON COLUMN stage.registrations.date_oper IS 'Дата операции';
COMMENT ON COLUMN stage.registrations.product IS 'Запись в ПТС названия ТС';
COMMENT ON COLUMN stage.registrations."year" IS 'Год выпуска';
COMMENT ON COLUMN stage.registrations.vin IS 'ВИН';
COMMENT ON COLUMN stage.registrations.num_body IS 'Номер кузова';
COMMENT ON COLUMN stage.registrations.num_engine IS 'Номер двигателя';
COMMENT ON COLUMN stage.registrations.num_shassis IS 'Номер шасси';
COMMENT ON COLUMN stage.registrations.code_tech_oper IS 'Код технологической операции ГИБДД';
COMMENT ON COLUMN stage.registrations.affiliation IS 'Принадлежность к ФЛ или ЮЛ';
COMMENT ON COLUMN stage.registrations.power IS 'Мощность';
COMMENT ON COLUMN stage.registrations.volume IS 'Объем';
COMMENT ON COLUMN stage.registrations.type_engine IS 'Тип двигателя';
COMMENT ON COLUMN stage.registrations.wheel IS 'Провосторонность руля';
COMMENT ON COLUMN stage.registrations.code_ts IS 'Код транспортного средства';
COMMENT ON COLUMN stage.registrations."comment" IS 'Комментарий';
COMMENT ON COLUMN stage.registrations.max_massa IS 'Максимальная масса';
COMMENT ON COLUMN stage.registrations.min_massa IS 'Масса без нагрузки';
COMMENT ON COLUMN stage.registrations.full_name_owner IS 'Наименование владельца';
COMMENT ON COLUMN stage.registrations.inn IS 'ИНН';
COMMENT ON COLUMN stage.registrations.district_owner IS 'Район владельца';
COMMENT ON COLUMN stage.registrations.city_owner IS 'Город владельца';
COMMENT ON COLUMN stage.registrations.brand IS 'Производитель';
COMMENT ON COLUMN stage.registrations.model IS 'Модель';
COMMENT ON COLUMN stage.registrations."class" IS 'Класс модели';
COMMENT ON COLUMN stage.registrations.type_model IS 'Тип модели';
COMMENT ON COLUMN stage.registrations.origin IS 'Просхождение';
COMMENT ON COLUMN stage.registrations.body IS 'Кузов';
COMMENT ON COLUMN stage.registrations.formula IS 'Колесная формула';
COMMENT ON COLUMN stage.registrations.id1 IS 'СОАТО + id1 = pk';
COMMENT ON COLUMN stage.registrations.district_new IS 'Новый район владельца';
COMMENT ON COLUMN stage.registrations.factory IS 'Надстройщик';
COMMENT ON COLUMN stage.registrations.description_kind IS 'Описание (вид)';
COMMENT ON COLUMN stage.registrations.description_type IS 'Описание (тип)';
COMMENT ON COLUMN stage.registrations.country_brand IS 'Страна марки ТС';
COMMENT ON COLUMN stage.registrations.eco_type IS 'Экология';
COMMENT ON COLUMN stage.registrations.body_type IS 'Тип кузова';
COMMENT ON COLUMN stage.registrations.okved2 IS 'Код ОКВЭД2';
COMMENT ON COLUMN stage.registrations.activity_type IS 'Вид деятельности по ОКВЭД2';
COMMENT ON COLUMN stage.registrations.class_new IS 'Класс новый';
COMMENT ON COLUMN stage.registrations.subclass IS 'Подкласс';
COMMENT ON COLUMN stage.registrations.coato_owner IS 'СОАТО владельца';
COMMENT ON COLUMN stage.registrations.form_ownership IS 'Форма собственности';


-----------------------DDS---------------------------
DROP TABLE IF EXISTS dds.internal_market_registration;
DROP TABLE IF EXISTS dds.internal_market_transport;
DROP TABLE IF EXISTS dds.internal_market_brands;
DROP TABLE IF EXISTS dds.internal_market_owner;


CREATE TABLE dds.internal_market_brands (
	id serial NOT NULL UNIQUE,
	brand varchar NULL,
	country varchar NULL,
	ts timestamp DEFAULT now(),
	
	UNIQUE(brand, country)
	
)
DISTRIBUTED REPLICATED;

-- Table comment
COMMENT ON TABLE dds.internal_market_brands IS 'Производитель ТС';

-- Column comments
COMMENT ON COLUMN dds.internal_market_brands.brand IS 'Название бренда';
COMMENT ON COLUMN dds.internal_market_brands. country IS 'Страна бренда';


CREATE TABLE dds.internal_market_transport (
	id serial NOT NULL UNIQUE,
	id_brand int  REFERENCES dds.internal_market_brands(id),    --Производитель
	type_ts varchar NULL,    	 								--Тип т/с
	product varchar NULL,    	 								--Товар
	"year" varchar NULL,         								--Год
	vin varchar NULL,            								--VIN
	num_body varchar NULL,       								--N куз
	num_engine varchar NULL,     								--N двиг
	num_shassis varchar NULL,    								--N шасси
	power varchar NULL,          								--Мощность
	volume varchar NULL,         								--Объем
	type_engine varchar NULL,    								--Тип двигателя
	wheel varchar NULL,          								--Правосторонность руля
	code_ts varchar NULL,        								--Код т/с
	"comment" text NULL,         								--Комментарий к коду тс
	max_massa varchar NULL,      								--Максимальная масса
	min_massa varchar NULL,      								--Масса без нагрузки
	model varchar NULL,                							--Модель
	"class" varchar NULL,              							--Класс
	type_model varchar NULL,       	   							--Тип
	origin varchar NULL,           	   							--Происхождение
	body varchar NULL,                 							--Кузов
	formula varchar NULL,              							--Кол фор-ла
	factory varchar NULL,              							--Надстройщик
	description_kind text NULL,        							--Описание(вид)
	description_type text NULL,        							--Описание(тип)
	eco_type varchar NULL,             							--Экология
	body_type varchar NULL,            							--Тип кузова
	class_new varchar NULL,            							--Класс новый
	subclass varchar NULL,             							--Подкласс
	ts timestamp DEFAULT NOW()
)
DISTRIBUTED BY(id);

-- Table comment
COMMENT ON TABLE dds.internal_market_transport IS 'Регистрируемое ТС';

-- Columns comments
COMMENT ON COLUMN dds.internal_market_transport.id_brand IS 'Производитель ИД';
COMMENT ON COLUMN dds.internal_market_transport.type_ts IS 'Тип ТС';
COMMENT ON COLUMN dds.internal_market_transport.product IS 'Запись в ПТС названия ТС';
COMMENT ON COLUMN dds.internal_market_transport."year" IS 'Год выпуска';
COMMENT ON COLUMN dds.internal_market_transport.vin IS 'ВИН';
COMMENT ON COLUMN dds.internal_market_transport.num_body IS 'Номер кузова';
COMMENT ON COLUMN dds.internal_market_transport.num_engine IS 'Номер двигателя';
COMMENT ON COLUMN dds.internal_market_transport.num_shassis IS 'Номер шасси';
COMMENT ON COLUMN dds.internal_market_transport.power IS 'Мощность';
COMMENT ON COLUMN dds.internal_market_transport.volume IS 'Объем';
COMMENT ON COLUMN dds.internal_market_transport.type_engine IS 'Тип двигателя';
COMMENT ON COLUMN dds.internal_market_transport.wheel IS 'Провосторонность руля';
COMMENT ON COLUMN dds.internal_market_transport.code_ts IS 'Код транспортного средства';
COMMENT ON COLUMN dds.internal_market_transport."comment" IS 'Комментарий';
COMMENT ON COLUMN dds.internal_market_transport.max_massa IS 'Максимальная масса';
COMMENT ON COLUMN dds.internal_market_transport.min_massa IS 'Масса без нагрузки';
COMMENT ON COLUMN dds.internal_market_transport.model IS 'Модель';
COMMENT ON COLUMN dds.internal_market_transport."class" IS 'Класс модели';
COMMENT ON COLUMN dds.internal_market_transport.type_model IS 'Тип модели';
COMMENT ON COLUMN dds.internal_market_transport.origin IS 'Просхождение';
COMMENT ON COLUMN dds.internal_market_transport.body IS 'Кузов';
COMMENT ON COLUMN dds.internal_market_transport.formula IS 'Колесная формула';
COMMENT ON COLUMN dds.internal_market_transport.factory IS 'Надстройщик';
COMMENT ON COLUMN dds.internal_market_transport.description_kind IS 'Описание (вид)';
COMMENT ON COLUMN dds.internal_market_transport.description_type IS 'Описание (тип)';
COMMENT ON COLUMN dds.internal_market_transport.eco_type IS 'Экология';
COMMENT ON COLUMN dds.internal_market_transport.body_type IS 'Тип кузова';
COMMENT ON COLUMN dds.internal_market_transport.class_new IS 'Класс новый';
COMMENT ON COLUMN dds.internal_market_transport.subclass IS 'Подкласс';


CREATE TABLE dds.internal_market_owner (
	id serial NOT NULL UNIQUE,
	full_name_owner varchar NULL,	   --Наименование владельца
	inn varchar NULL,                  --ИНН
	district_owner varchar NULL,       --Район
	city_owner varchar NULL,           --Город
	district_new varchar NULL,     	   --Район новый
	okved2 varchar NULL,               --Код ОКВЭД2
	activity_type varchar NULL,        --Вид деятельности по ОКВЭД2
	coato_owner varchar NULL,          --СОАТО владельца
	form_ownership varchar NULL,       --Форма собственности
	affiliation varchar NULL,    	   --Принадлежность
	ts TIMESTAMP DEFAULT NOW()
)
DISTRIBUTED BY (id);

-- Table comment

COMMENT ON TABLE dds.internal_market_owner IS 'Регистрация транспортных средств';

-- Column comments

COMMENT ON COLUMN dds.internal_market_owner.full_name_owner IS 'Наименование владельца';
COMMENT ON COLUMN dds.internal_market_owner.inn IS 'ИНН';
COMMENT ON COLUMN dds.internal_market_owner.district_owner IS 'Район владельца';
COMMENT ON COLUMN dds.internal_market_owner.city_owner IS 'Город владельца';
COMMENT ON COLUMN dds.internal_market_owner.district_new IS 'Новый район владельца';
COMMENT ON COLUMN dds.internal_market_owner.okved2 IS 'Код ОКВЭД2';
COMMENT ON COLUMN dds.internal_market_owner.activity_type IS 'Вид деятельности по ОКВЭД2';
COMMENT ON COLUMN dds.internal_market_owner.coato_owner IS 'СОАТО владельца';
COMMENT ON COLUMN dds.internal_market_owner.form_ownership IS 'Форма собственности';


DROP TABLE IF EXISTS dds.internal_market_registration;
CREATE TABLE dds.internal_market_registration (
	id serial NOT NULL,
	id_transport int NOT NULL, 
	id_owner int NOT NULL, 
	coato_registr varchar NULL,      --СОАТО
	date_reg date NULL,      	 	 --Дата регистрации
	date_oper date NULL,     	 	 --Дата операции
	code_tech_oper varchar NULL, 	 --Первичность
	ts TIMESTAMP DEFAULT NOW(),

	CONSTRAINT internal_market_registration_id_transport_fk FOREIGN KEY(id_transport) REFERENCES dds.internal_market_transport (id),
	CONSTRAINT internal_market_registration_id_owner_fk FOREIGN KEY(id_owner) REFERENCES dds.internal_market_owner (id)
)
DISTRIBUTED BY(id_transport)
PARTITION BY RANGE(date_oper)
(
	PARTITION P_01_2021 START('2021-01-01') INCLUSIVE END('2021-02-01') EXCLUSIVE
	,PARTITION P_02_2021 START('2021-02-01') INCLUSIVE END('2021-03-01') EXCLUSIVE
	,PARTITION P_03_2021 START('2021-03-01') INCLUSIVE END('2021-04-01') EXCLUSIVE
	,PARTITION P_04_2021 START('2021-04-01') INCLUSIVE END('2021-05-01') EXCLUSIVE
	,PARTITION P_05_2021 START('2021-05-01') INCLUSIVE END('2021-06-01') EXCLUSIVE
	,PARTITION P_06_2021 START('2021-06-01') INCLUSIVE END('2021-07-01') EXCLUSIVE
	,PARTITION P_07_2021 START('2021-07-01') INCLUSIVE END('2021-08-01') EXCLUSIVE
	,PARTITION P_08_2021 START('2021-08-01') INCLUSIVE END('2021-09-01') EXCLUSIVE
	,PARTITION P_09_2021 START('2021-09-01') INCLUSIVE END('2021-10-01') EXCLUSIVE
	,PARTITION P_10_2021 START('2021-10-01') INCLUSIVE END('2021-11-01') EXCLUSIVE
	,PARTITION P_11_2021 START('2021-11-01') INCLUSIVE END('2021-12-01') EXCLUSIVE
	,PARTITION P_12_2021 START('2021-12-01') INCLUSIVE END('2022-01-01') EXCLUSIVE
	
	,PARTITION P_01_2022 START('2022-01-01') INCLUSIVE END('2022-02-01') EXCLUSIVE
	,PARTITION P_02_2022 START('2022-02-01') INCLUSIVE END('2022-03-01') EXCLUSIVE
	,PARTITION P_03_2022 START('2022-03-01') INCLUSIVE END('2022-04-01') EXCLUSIVE
	,PARTITION P_04_2022 START('2022-04-01') INCLUSIVE END('2022-05-01') EXCLUSIVE
	,PARTITION P_05_2022 START('2022-05-01') INCLUSIVE END('2022-06-01') EXCLUSIVE
	,PARTITION P_06_2022 START('2022-06-01') INCLUSIVE END('2022-07-01') EXCLUSIVE
	,PARTITION P_07_2022 START('2022-07-01') INCLUSIVE END('2022-08-01') EXCLUSIVE
	,PARTITION P_08_2022 START('2022-08-01') INCLUSIVE END('2022-09-01') EXCLUSIVE
	,PARTITION P_09_2022 START('2022-09-01') INCLUSIVE END('2022-10-01') EXCLUSIVE
	,PARTITION P_10_2022 START('2022-10-01') INCLUSIVE END('2022-11-01') EXCLUSIVE
	,PARTITION P_11_2022 START('2022-11-01') INCLUSIVE END('2022-12-01') EXCLUSIVE
	,PARTITION P_12_2022 START('2022-12-01') INCLUSIVE END('2023-01-01') EXCLUSIVE

	,PARTITION P_01_2023 START('2023-01-01') INCLUSIVE END('2023-02-01') EXCLUSIVE
	,PARTITION P_02_2023 START('2023-02-01') INCLUSIVE END('2023-03-01') EXCLUSIVE
	,PARTITION P_03_2023 START('2023-03-01') INCLUSIVE END('2023-04-01') EXCLUSIVE
	,PARTITION P_04_2023 START('2023-04-01') INCLUSIVE END('2023-05-01') EXCLUSIVE
	,PARTITION P_05_2023 START('2023-05-01') INCLUSIVE END('2023-06-01') EXCLUSIVE
	,PARTITION P_06_2023 START('2023-06-01') INCLUSIVE END('2023-07-01') EXCLUSIVE
	,PARTITION P_07_2023 START('2023-07-01') INCLUSIVE END('2023-08-01') EXCLUSIVE
	,PARTITION P_08_2023 START('2023-08-01') INCLUSIVE END('2023-09-01') EXCLUSIVE
	,PARTITION P_09_2023 START('2023-09-01') INCLUSIVE END('2023-10-01') EXCLUSIVE
	,PARTITION P_10_2023 START('2023-10-01') INCLUSIVE END('2023-11-01') EXCLUSIVE
	,PARTITION P_11_2023 START('2023-11-01') INCLUSIVE END('2023-12-01') EXCLUSIVE
	,PARTITION P_12_2023 START('2023-12-01') INCLUSIVE END('2024-01-01') EXCLUSIVE
);

-- Table comment
COMMENT ON TABLE dds.internal_market_registration IS ' Регистрации ТС';

-- Columns comments
COMMENT ON COLUMN dds.internal_market_registration.date_reg IS 'Дата регистрации';
COMMENT ON COLUMN dds.internal_market_registration.date_oper IS 'Дата операции';
COMMENT ON COLUMN dds.internal_market_registration.code_tech_oper IS 'Идентификатор кода технической операции';
