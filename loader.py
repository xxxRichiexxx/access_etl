from smb.SMBConnection import SMBConnection
import pyodbc
import psycopg2
import psycopg2.extras
import datetime as dt
import os


def access_loader(
    share_hostname,
    username,
    password,
    domain_name,
    share,
    file_path,
    local_file_path,
    access_table,
    dwh_host,
    dwh_port,
    dwh_database,
    dwh_scheme,
    dwh_user,
    dwh_password,
    dwh_table,
    dwh_columns,
):
    # Создание соединения SMB
    source_conn = SMBConnection(username, password, '', share_hostname, domain=domain_name, use_ntlm_v2=True)
    source_conn.connect(share_hostname)  # Подключение к хосту

    # Записываем файл из источника в приемник
    with open(local_file_path, 'wb') as file_obj:
        source_conn.retrieveFile(share, file_path, file_obj)

    # 3. Подключитесь к базе данных Access:
    access_conn_str = r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};' + f'DBQ={local_file_path};'

    print('Читаем источник.')
    with pyodbc.connect(access_conn_str) as access_conn:
        with access_conn.cursor() as cursor:

            cursor.execute(
                f"""
                SELECT MIN("Дата операции") FROM {access_table}
                """
            )
            min_date = cursor.fetchone()[0]
            print(min_date)

            cursor.execute(
                f"""
                SELECT MAX("Дата операции") FROM {access_table}
                """
            )
            max_date = cursor.fetchone()[0]
            print(max_date)
    
            query = f'SELECT * FROM {access_table}'
            cursor.execute(query)
            data = cursor.fetchall()

    access_conn.close()

    dwh_conn = psycopg2.connect(
        host=dwh_host,
        port=dwh_port,
        database=dwh_database,
        user=dwh_user,
        password=dwh_password,
    )
    
    with dwh_conn:
        with dwh_conn.cursor() as dwh_cur:

            print('Обеспечиваем идемпотентность.')

            dwh_cur.execute(
                f"""
                DELETE FROM {dwh_scheme}.{dwh_table} WHERE date_oper BETWEEN '{min_date}' AND '{max_date}';
                """
            )

            dwh_cur.execute(
                f"""
                SELECT 1
                FROM pg_partitions
                WHERE schemaname = 'stage'
                    AND tablename = '{dwh_table}'
                    AND partitionname = 'p_{min_date.month}_{min_date.year}';
                """
            )

            there_is_pt = dwh_cur.fetchone()
            print('Проверяем наличие партиции', there_is_pt)

            if not there_is_pt:

                start_part_dt = min_date.replace(day=1)
                print('start_part_dt', start_part_dt)
                end_part_dt = (min_date.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
                print('end_part_dt', end_part_dt)

                dwh_cur.execute(
                    f"""
                    ALTER TABLE {dwh_scheme}.{dwh_table}
                    ADD PARTITION p_{min_date.month}_{min_date.year}
                    START('{start_part_dt}') INCLUSIVE END('{end_part_dt}') EXCLUSIVE;
                    """
                )

            dwh_columns = ','.join(dwh_columns)                

            print('Осуществляем вставку данных.')
            insert_stmt = f"INSERT INTO {dwh_scheme}.{dwh_table} ({dwh_columns}) VALUES %s"
            psycopg2.extras.execute_values(dwh_cur, insert_stmt, data)
            print('Вставка данных завершена.')

    os.remove(local_file_path)


if __name__ == '__main__':

    # Замените следующие значения соответствующими вашим настройкам smb-сервера
    share_hostname = 'napp2750'  # Имя хоста, на котором располагается файл или имя корня nfs
    username = 'PowerBI_integration'  # Ваше имя пользователя
    password = 'n0l2mgucgUrRRUassTjP'  # Ваш пароль
    domain_name = 'st'  # Имя вашего домена
    # название шары
    share = 'public'
    # это путь до файла-источника без учета родительской папки-шары
    file_path = r'\STT\Общая\Дирекция по маркетингу и развитию продаж\BI\Регистрация_обработка\Исходки\2022\ноябрь\ноябрь.mdb'
    # это путь до файла-приемника
    local_file_path = r'C:\\temp\\test.mdb'

    access_table = 'test'

    dwh_host = 'vs-dwh-gpm2.st.tech'
    dwh_port = '5432'
    dwh_database = 'test_dwh'
    dwh_scheme = 'stage'
    dwh_user = 'shveynikovab'
    dwh_password = 'fk2QVnJH8i'
    dwh_table = 'registrations'
    
    dwh_columns = [
        'coato_registr',
        'type_ts',
        'date_reg',      	          
        'date_oper',     	         
        'product',    	          
        'year',                   
        'vin',                       
        'num_body',                  
        'num_engine',                 
        'num_shassis',    
        'code_tech_oper', 	 
        'affiliation',    
        'power',           
        'volume',           
        'type_engine',     
        'wheel',          
        'code_ts',          
        "comment",         
        'max_massa',        
        'min_massa',         
        'full_name_owner',
        'inn',                 
        'district_owner',       
        'city_owner',           
        'brand',              
        'model',                
        "class",              
        'type_model',       	 
        'origin',           	  
        'body',                
        'formula',             
        'id1',                  	
        'district_new',     	  
        'factory',             
        'description_kind',       
        'description_type',        
        'country_brand',      
        'eco_type',             
        'body_type',            
        'okved2',              
        'activity_type',        
        'class_new',           
        'subclass',             
        'coato_owner',
        'form_ownership',
    ]              
    
    access_loader(
        share_hostname,
        username,
        password,
        domain_name,
        share,
        file_path,
        local_file_path,
        access_table,
        dwh_host,
        dwh_port,
        dwh_database,
        dwh_scheme,
        dwh_user,
        dwh_password,
        dwh_table,
        dwh_columns,
    )
