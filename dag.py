from smb.SMBConnection import SMBConnection
import pyodbc
import psycopg2
import psycopg2.extras
import datetime as dt
import os
import pandas_access as mdb
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.operators.python import BranchPythonOperator

from isc_etl.scripts.collable import etl, date_check, contracting_calculate


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


    print('Читаем источник.')
    df = mdb.read_table(local_file_path, "test", dtype=object)
    print(df)
    # with pyodbc.connect(access_conn_str) as access_conn:
    #     with access_conn.cursor() as cursor:

    #         cursor.execute(
    #             f"""
    #             SELECT MIN("Дата операции") FROM {access_table}
    #             """
    #         )
    #         min_date = cursor.fetchone()[0]
    #         print(min_date)

    #         cursor.execute(
    #             f"""
    #             SELECT MAX("Дата операции") FROM {access_table}
    #             """
    #         )
    #         max_date = cursor.fetchone()[0]
    #         print(max_date)
    
    #         query = f'SELECT * FROM {access_table}'
    #         cursor.execute(query)
    #         data = cursor.fetchall()

    # access_conn.close()

    # dwh_conn = psycopg2.connect(
    #     host=dwh_host,
    #     port=dwh_port,
    #     database=dwh_database,
    #     user=dwh_user,
    #     password=dwh_password,
    # )
    
    # with dwh_conn:
    #     with dwh_conn.cursor() as dwh_cur:

    #         print('Обеспечиваем идемпотентность.')

    #         dwh_cur.execute(
    #             f"""
    #             DELETE FROM {dwh_scheme}.{dwh_table} WHERE date_oper BETWEEN '{min_date}' AND '{max_date}';
    #             """
    #         )

    #         dwh_cur.execute(
    #             f"""
    #             SELECT 1
    #             FROM pg_partitions
    #             WHERE schemaname = 'stage'
    #                 AND tablename = '{dwh_table}'
    #                 AND partitionname = 'p_{min_date.month}_{min_date.year}';
    #             """
    #         )

    #         there_is_pt = dwh_cur.fetchone()
    #         print('Проверяем наличие партиции', there_is_pt)

    #         if not there_is_pt:

    #             start_part_dt = min_date.replace(day=1)
    #             print('start_part_dt', start_part_dt)
    #             end_part_dt = (min_date.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    #             print('end_part_dt', end_part_dt)

    #             dwh_cur.execute(
    #                 f"""
    #                 ALTER TABLE {dwh_scheme}.{dwh_table}
    #                 ADD PARTITION p_{min_date.month}_{min_date.year}
    #                 START('{start_part_dt}') INCLUSIVE END('{end_part_dt}') EXCLUSIVE;
    #                 """
    #             )

    #         dwh_columns = ','.join(dwh_columns)                

    #         print('Осуществляем вставку данных.')
    #         insert_stmt = f"INSERT INTO {dwh_scheme}.{dwh_table} ({dwh_columns}) VALUES %s"
    #         psycopg2.extras.execute_values(dwh_cur, insert_stmt, data)
    #         print('Вставка данных завершена.')

    # os.remove(local_file_path)


default_args = {
    'owner': 'Швейников Андрей',
    'email': ['xxxRichiexxx@yandex.ru'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'access_load',
        default_args=default_args,
        description='Получение данных из файлов ACCESS.',
        start_date=dt.datetime(2023, 8, 13),
        schedule_interval=None,
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        PythonOperator(
            task_id=f'get_data',
            python_callable=access_loader,
            op_kwargs={
                'share_hostname': 'napp2750',
                'username': 'PowerBI_integration',
                'password': 'n0l2mgucgUrRRUassTjP',  
                'domain_name': 'st', 
                'share': 'public',
                'file_path': r'\STT\Общая\Дирекция по маркетингу и развитию продаж\BI\Регистрация_обработка\Исходки\2022\ноябрь\ноябрь.mdb',
                'local_file_path': r'/tmp/access/access.mdb',
                'access_table': 'test',
                'dwh_host': 'vs-dwh-gpm2.st.tech',
                'dwh_port': '5432',
                'dwh_database': 'test_dwh',
                'dwh_scheme': 'stage',
                'dwh_user': 'shveynikovab',
                'dwh_password': 'fk2QVnJH8i',
                'dwh_table': 'registrations',
                'dwh_columns': dwh_columns,
            },
        )

    with TaskGroup('Загрузка_данных_в_dds_слой') as data_to_dds:

        pass
        

    with TaskGroup('Загрузка_данных_в_dm_слой') as data_to_dm:

        pass

        
    with TaskGroup('Проверки') as data_checks:

        pass


    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> data_to_dm >> data_checks >> end

    # Если вы хотите использовать Linux для чтения данных из базы данных Access, вам потребуется другой драйвер, так как драйвер Microsoft Access не поддерживается в Linux. Вместо него вы можете использовать драйвер mdbtools.

    # Вот пример кода с использованием драйвера mdbtools вместо pyodbc:

    # 1. Установите необходимые пакеты для подключения к базе данных Access через mdbtools:

    # sudo apt-get update
    # sudo apt-get install mdbtools libmdb-dev


    # 2. Затем в коде Python вы можете использовать модуль pmdb для чтения таблицы с помощью Pandas:

    # import pandas as pd
    # import pmdb

    # # Задайте путь к вашей базе данных Access
    # database_path = '/путь/к/вашей/базе/данных.mdb'

    # # Создайте подключение к базе данных
    # conn = pmdb.connect(database_path)

    # # Укажите имя таблицы, которую вы хотите прочитать
    # table_name = 'Название_таблицы'

    # # Используйте функцию read_table() из Pandas для чтения таблицы в DataFrame
    # df = pd.read_table(conn, table_name)

    # # Выведите содержимое DataFrame
    # print(df)


    # Обратите внимание, что нужно изменить database_path на путь к файлу базы данных Access и table_name на имя таблицы, которую вы хотите прочитать.

    # Таким образом, используя драйвер mdbtools вместо pyodbc, вы сможете читать данные из базы данных Access под Linux.

