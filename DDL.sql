DROP TABLE IF EXISTS stage.registrations;
CREATE TABLE stage.registrations (
	coato_registr int NULL,      --СОАТО
	type_ts varchar NULL,    	 --Тип т/с
	date_reg date NULL,      	 --Дата регистрации
	date_oper date NULL,     	 --Дата операции
	product varchar NULL,    	 --Товар
	"year" int NULL,             --Год
	vin varchar NULL,            --VIN
	num_body varchar NULL,       --N куз
	num_engine varchar NULL,     --N двиг
	num_shassis varchar NULL,    --N шасси
	code_tech_oper int NULL, 	 --Первичность
	affiliation int NULL,    	 --Принадлежность
	power float NULL,            --Мощность
	volume int NULL,             --Объем
	type_engine int NULL,        --Тип двигателя
	wheel varchar NULL,          --Правосторонность руля
	code_ts int NULL,            --Код т/с
	"comment" text NULL,         --Комментарий к коду тс
	max_massa int NULL,          --Максимальная масса
	min_massa int NULL,          --Масса без нагрузки
	full_name_owner varchar NULL,--Наименование владельца
	inn varchar NULL,                  --ИНН
	district_owner varchar NULL,       --Район
	city_owner varchar NULL,           --Город
	brand varchar NULL,                --Марка
	model varchar NULL,                --Модель
	"class" varchar NULL,              --Класс
	type_model varchar NULL,       	   --Тип
	origin varchar NULL,           	   --Происхождение
	body varchar NULL,                 --Кузов
	formula varchar NULL,              --Кол фор-ла
	id1 int NULL,                  	   --ID1
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
	coato_owner int NULL,              --СОАТО владельца
	form_ownership varchar NULL,       --Форма собственности

	UNIQUE(coato_registr, id1)
);
