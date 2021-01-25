from datetime import datetime, timedelta
import logging
import pandas as pd

from airflow import DAG
# from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.capstone_project_plugin import (
    StageToRedshiftOperator,
    LoadTableOperator,
    DataQualityOperator
)
from airflow.hooks.S3_hook import S3Hook
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime.now() - timedelta(days=1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dag = DAG(
    dag_id='dend_capstone_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@yearly'
)


def extract_chem_production(
    s3_bucket, source_key, local_download_path,
    local_upload_path, target_key
):
    """Extract agricultural chemical production index data from a json file.
    """
    # create an S3 hook
    s3_hook = S3Hook(aws_conn_id='aws_credentials')

    logging.info(f'Downdloading {source_key} from {s3_bucket}')
    # get the S3 object
    result = s3_hook.read_key(key=source_key, bucket_name=s3_bucket)

    with open(local_download_path, 'w') as file:
        file.write(result)

    logging.info(f'Cleaning the data from {source_key}')
    # read the json_file to a pandas dataframe
    eia_steo = pd.read_json(local_download_path, lines=True)

    # get the agricultural chemical production index record
    ann_chem_prod = eia_steo[
        eia_steo.name == 'Agricultural Chemicals Production Index, Annual'
    ].copy()

    # explode the column so that each record forms a row
    ann_chem_prod = ann_chem_prod.explode('data')

    # reset the dataframe index
    ann_chem_prod.reset_index(drop=True, inplace=True)

    # extract the year and value data from the 'data' column
    ann_chem_prod['year'] = ann_chem_prod.data.apply(lambda x: x[0]).astype(int)
    ann_chem_prod['value'] = ann_chem_prod.data.apply(lambda x: x[1]).astype(float)

    # save the data as csv locally
    ann_chem_prod.to_csv(local_upload_path, index=False)

    logging.info(f'Uploading the cleaned file to {s3_bucket}')
    # upload the csv file to S3
    s3_hook.load_file(
        filename=local_upload_path, key=target_key,
        bucket_name=s3_bucket, replace=True
    )


start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

extract_chem_production = PythonOperator(
    task_id='Extract_chem_production',
    dag=dag,
    python_callable=extract_chem_production,
    op_kwargs={
        's3_bucket': 'dend-capstone-bucket',
        'source_key': 'STEO.txt',
        'local_download_path': '/tmp/STEO.txt',
        'local_upload_path': '/tmp/agri_chem_prod_index.csv',
        'target_key': 'agri_chem_prod_index.csv'
    }
)


stage_temperature_to_redshift = StageToRedshiftOperator(
    task_id='Stage_temperature',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_temperature',
    s3_bucket='dend-capstone-bucket',
    s3_key='GlobalLandTemperaturesByCountry.csv',
    region='ap-southeast-1',
    delimiter=',',
    ignore_header=1,
    date_format='YYYY-MM-DD',
    create_sql=SqlQueries.stage_temperature_create,
    truncate=True
)

stage_crop_production_to_redshift = StageToRedshiftOperator(
    task_id='Stage_crop_production',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_crop_production',
    s3_bucket='dend-capstone-bucket',
    s3_key='crops_production/Production_Crops_E_All_Data_(Normalized).csv',
    region='ap-southeast-1',
    delimiter=',',
    ignore_header=1,
    create_sql=SqlQueries.stage_crop_production_create,
    truncate=True
)

stage_flag_to_redshift = StageToRedshiftOperator(
    task_id='Stage_flag',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_flag',
    s3_bucket='dend-capstone-bucket',
    s3_key='crops_production/Production_Crops_E_Flags.csv',
    region='ap-southeast-1',
    delimiter=',',
    ignore_header=1,
    create_sql=SqlQueries.stage_flag_create,
    truncate=True
)

stage_chem_production_to_redshift = StageToRedshiftOperator(
    task_id='Stage_chem_production',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='stage_agri_chemical',
    s3_bucket='dend-capstone-bucket',
    s3_key='agri_chem_prod_index.csv',
    region='ap-southeast-1',
    delimiter=',',
    ignore_header=1,
    time_format='auto',
    create_sql=SqlQueries.stage_agri_chem_create,
    truncate=True
)

check_stage_tables = DataQualityOperator(
    task_id='Check_stage_tables',
    dag=dag,
    redshift_conn_id='redshift',
    tables=[
        'stage_temperature', 'stage_crop_production',
        'stage_flag', 'stage_agri_chemical'
    ]
)

load_temperature_table = LoadTableOperator(
    task_id='Load_temperature_table',
    dag=dag,
    redshift_conn_id='redshift',
    create_sql=SqlQueries.temperature_create,
    table='temperature',
    select_sql=SqlQueries.temperature_insert,
    truncate=True
)

load_chem_production_index_table = LoadTableOperator(
    task_id='Load_chemical_production_index_table',
    dag=dag,
    redshift_conn_id='redshift',
    create_sql=SqlQueries.agri_chem_prod_index_create,
    table='agri_chem_prod_index',
    select_sql=SqlQueries.agri_chem_prod_index_insert,
    truncate=True
)

load_item_table = LoadTableOperator(
    task_id='Load_item_table',
    dag=dag,
    redshift_conn_id='redshift',
    create_sql=SqlQueries.item_create,
    table='item',
    select_sql=SqlQueries.item_insert,
    truncate=True
)

load_element_table = LoadTableOperator(
    task_id='Load_item_table',
    dag=dag,
    redshift_conn_id='redshift',
    create_sql=SqlQueries.element_create,
    table='element',
    select_sql=SqlQueries.element_insert,
    truncate=True
)

load_flag_table = LoadTableOperator(
    task_id='Load_flag_table',
    dag=dag,
    redshift_conn_id='redshift',
    create_sql=SqlQueries.flag_create,
    table='flag',
    select_sql=SqlQueries.flag_insert,
    truncate=True
)

check_tables = DataQualityOperator(
    task_id='Check_tables',
    dag=dag,
    redshift_conn_id='redshift',
    tables=[
        'temperature', 'agri_chem_prod_index',
        'item', 'element', 'flag'
    ]
)

load_crop_production_table = LoadTableOperator(
    task_id='Load_crop_production_table',
    dag=dag,
    redshift_conn_id='redshift',
    create_sql=SqlQueries.crop_production_create,
    table='crop_production',
    select_sql=SqlQueries.crop_production_insert,
    truncate=True
)

check_crop_production_table = DataQualityOperator(
    task_id='Check_crop_production_table',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['crop_production']
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> extract_chem_production >> stage_chem_production_to_redshift

start_operator >> [
    stage_temperature_to_redshift,
    stage_crop_production_to_redshift,
    stage_flag_to_redshift
]

[
    stage_temperature_to_redshift,
    stage_crop_production_to_redshift,
    stage_flag_to_redshift,
    stage_chem_production_to_redshift
] >> check_stage_tables

check_stage_tables >> [
    load_temperature_table, load_chem_production_index_table,
    load_item_table, load_element_table, load_flag_table
] >> check_tables

check_tables >> load_crop_production_table
load_crop_production_table >> check_crop_production_table >> end_operator
