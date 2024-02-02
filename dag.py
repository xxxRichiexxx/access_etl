from smb.SMBConnection import SMBConnection
import psycopg2
import psycopg2.extras
import datetime as dt
import os
from urllib.parse import quote
import datetime as dt
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator


smb_con = BaseHook.get_connection('STT SMB')
dwh_con = BaseHook.get_connection('greenplum')           


def access_loader(
    smb_hostname,
    smb_username,
    smb_password,
    smb_domain_name,
    smb_share,
    smb_file_path,
    airflow_local_file_path,
    dwh_host,
    dwh_port,
    dwh_database,
    dwh_scheme,
    dwh_user,
    dwh_password,
    dwh_table,
    dwh_columns,
    smb_file_header=False,
    **context,
):
    # Создание соединения SMB
    source_conn = SMBConnection(
        smb_username, 
        smb_password,
        '',
        smb_hostname,
        domain=smb_domain_name,
        use_ntlm_v2=True,
    )
    try:
        source_conn.connect(smb_hostname)  # Подключение к SMB-хосту

        print(('ВНИМАНИЕ: данный ДАГ загружает в таблицу хранилища stage.registrations CSV-файл.',
            'Путь к данному файлу хранится в переменной smb_file_path и задается через интерфейс airflow'))
        
        print(f'Копирую файл .csv из сетевой папки {smb_share+smb_file_path} на Airflow:', airflow_local_file_path)

        # Записываем файл из источника в приемник
        with open(airflow_local_file_path, 'wb') as file_obj:
            source_conn.retrieveFile(smb_share, smb_file_path, file_obj)
    except:
            raise Exception('Не могу получить целевой файл. Проверьте путь до файла.') 

    header = 0 if smb_file_header else None
    data = pd.read_csv(airflow_local_file_path, delimiter=';', header=header)
    data.columns = dwh_columns
    print('Получены следующие данные', data)

    data['date_oper'] = pd.to_datetime(data['date_oper'])
    min_date = min(data['date_oper'])
    print('Минимальная дата операции', min_date)
    max_date = max(data['date_oper'])
    print('Максимальная дата операции', max_date)

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
                DELETE FROM {dwh_scheme}.{dwh_table} 
                WHERE date_oper BETWEEN '{min_date}' AND '{max_date}';
                """
            )

            print('Проверяем наличие партиции')
            query = f"""
                SELECT 1
                FROM pg_partitions
                WHERE schemaname = '{dwh_scheme}'
                    AND tablename = '{dwh_table}'
                    AND partitionname = 'p_{min_date.month}_{min_date.year}';
                """
            dwh_cur.execute(
                query
            )

            there_is_pt = dwh_cur.fetchone()
            print(there_is_pt)

            if not there_is_pt:

                print('Партиция не обнаружена, создаем.')
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

            with open(airflow_local_file_path, 'r', newline='', encoding='utf-8') as csv_file:

                header = 'HEADER' if smb_file_header else ''
                copy_query = f"""
                              COPY {dwh_scheme}.{dwh_table} ({dwh_columns}) 
                              FROM STDIN WITH CSV DELIMITER ';' {header}
                              """
                dwh_cur.copy_expert(copy_query, csv_file)
            
            print('Вставка данных завершена.')

    print('Удаляем csv-файл с эйрфлоу')
    os.remove(airflow_local_file_path)

    print('Пушим min_date и max_date в XCom')
    context['ti'].xcom_push(key='min_date', value=str(min_date))
    context['ti'].xcom_push(key='max_date', value=str(max_date))


def partition_check(
    dwh_host,
    dwh_port,
    dwh_database,
    dwh_scheme,
    dwh_user,
    dwh_password,
    dwh_table,
    **context
):
    dwh_conn = psycopg2.connect(
        host=dwh_host,
        port=dwh_port,
        database=dwh_database,
        user=dwh_user,
        password=dwh_password,
    )

    min_date = context['ti'].xcom_pull(key='min_date', task_ids='Загрузка_данных_в_stage_слой.get_data')
    min_date = dt.datetime.strptime(min_date,"%Y-%m-%d %H:%M:%S")

    with dwh_conn:
        with dwh_conn.cursor() as dwh_cur:

            print('Проверяем наличие партиции')
            query = f"""
                SELECT 1
                FROM pg_partitions
                WHERE schemaname = '{dwh_scheme}'
                    AND tablename = '{dwh_table}'
                    AND partitionname = 'p_{min_date.month}_{min_date.year}';
                """
            print(query)
            dwh_cur.execute(
                query
            )

            there_is_pt = dwh_cur.fetchone()
            print(there_is_pt)

            if not there_is_pt:

                print('Партиция не обнаружена, создаем.')
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

                print('Партиция создана.')


default_args = {
    'owner': 'Швейников Андрей',
    'email': ['xxxRichiexxx@yandex.ru'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'registrations_load',
        default_args=default_args,
        description='Получение данных из файлов csv.',
        start_date=dt.datetime(2023, 8, 13),
        schedule_interval=None,
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        PythonOperator(
            task_id=f'get_data',
            python_callable=access_loader,
            op_kwargs={
                'smb_hostname': smb_con.host,
                'smb_username': smb_con.login,
                'smb_password': quote(smb_con.password),  
                'smb_domain_name': smb_con.schema, 
                'smb_share': 'public',
                'smb_file_path': Variable.get('smb_file_path'),
                'airflow_local_file_path': r'/tmp/access/data.csv',
                'dwh_host': dwh_con.host,
                'dwh_port': dwh_con.port,
                'dwh_database': dwh_con.schema,
                'dwh_scheme': 'stage',
                'dwh_user': dwh_con.login,
                'dwh_password': quote(dwh_con.password),
                'dwh_table': 'registrations',
                'dwh_columns': ['coato_registr',
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
                                'form_ownership'] ,
            },
        )

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        internal_market_brands = PostgresOperator(
            task_id='internal_market_brands',
            postgres_conn_id='greenplum',
            sql='dds.internal_market_brands.sql',
        )

        internal_market_transport = PostgresOperator(
            task_id='internal_market_transport',
            postgres_conn_id='greenplum',
            sql='dds.internal_market_transport.sql',
        )

        internal_market_owner = PostgresOperator(
            task_id='internal_market_owner',
            postgres_conn_id='greenplum',
            sql='dds.internal_market_owner.sql',
        )

        part_check = PythonOperator(
            task_id=f'partition_check',
            python_callable=partition_check,
            op_kwargs={
                'dwh_host': dwh_con.host,
                'dwh_port': dwh_con.port,
                'dwh_database': dwh_con.schema,
                'dwh_scheme': 'dds',
                'dwh_user': dwh_con.login,
                'dwh_password': quote(dwh_con.password),
                'dwh_table': 'internal_market_registration',
            }
        )

        internal_market_registration = PostgresOperator(
            task_id='internal_market_registration',
            postgres_conn_id='greenplum',
            sql='dds.internal_market_registration.sql',
        )

        internal_market_brands >> [internal_market_transport, internal_market_owner] >> part_check >> internal_market_registration

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        pass

        
    with TaskGroup('Проверки') as data_checks:

        pass


    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end