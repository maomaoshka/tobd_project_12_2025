from prefect import task, flow
import requests 
import boto3 # для работы с MinIO
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import os

from dask_jobs.transform import MinIODataProcessor, get_minio_storage_options
from utils.logging_config import get_logger

from prefect import task, flow, get_run_logger
import pandas as pd
import dask.dataframe as dd
from utils.load_to_dwh import load_data_to_postgres
import os

logger = get_logger(__name__)

# --- ЗАДАЧА №1: товарищ 1 ---
@task(retries=3, retry_delay_seconds=5)
def extract_data_to_minio():
    """
    Находит все CSV-файлы в локальной папке /data и загружает
    КАЖДЫЙ из них по отдельности в MinIO.
    """
    print("Начинаю извлечение данных из локальных CSV-файлов...")
    
    data_dir = 'data'

    try:
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError("В папке /app/data не найдено ни одного CSV-файла.")
        print(f"Обнаружено {len(csv_files)} CSV файлов для загрузки: {csv_files}")
    except FileNotFoundError as e:
        print(f"ОШИБКА: Папка {data_dir} не найдена. Проверьте volumes в docker-compose.yml")
        raise e

    minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:19090')
    
    minio_client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )

    bucket_name = "raw-data"
    try:
        minio_client.head_bucket(Bucket=bucket_name)
    except ClientError:
        print(f"Bucket {bucket_name} не найден. Создаю новый.")
        minio_client.create_bucket(Bucket=bucket_name)

    uploaded_file_paths = []
    for file_name in csv_files:
        local_file_path = os.path.join(data_dir, file_name)
        
        with open(local_file_path, 'rb') as f:
            data = f.read()

        minio_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=data
        )
        s3_path = f"s3://{bucket_name}/{file_name}"
        uploaded_file_paths.append(s3_path)
        print(f"Файл '{file_name}' успешно загружен в MinIO по пути: {s3_path}")
    
    print("Все файлы успешно загружены.")
    
    return uploaded_file_paths


# --- ЗАДАЧА №2: ЗАГЛУШКА ДЛЯ 3 товарища ---
@task
def transform_data_with_dask(uploaded_paths: list):
    logger = get_run_logger()
    logger.info("Начинаю трансформацию через MinIODataProcessor...")

    endpoint = os.getenv('MINIO_PORT_API', 'localhost:19090')
    storage_options = get_minio_storage_options(endpoint=endpoint)
    
    try:
        processor = MinIODataProcessor(storage_options)
        
        base_s3_path = "s3://raw-data"
        processed_ddf = processor.transform(base_s3_path)
        
        processed_ddf = processed_ddf.rename(columns={
            "Name": "wine_title",
            "Country": "country",
            "Region": "region",
            "Winery": "winery",
            "Rating": "rating",
            "NumberOfRatings": "number_of_ratings",
            "Price": "price",
            "Year": "year_of_production",
            "WineType": "wine_type"
        })
        
        logger.info(f"Трансформация завершена. Строк к загрузке: {len(processed_ddf)}")
        return processed_ddf

    except Exception as e:
        logger.error(f"Ошибка в задаче трансформации: {e}")
        raise e
    

# --- ЗАДАЧА №3: ЗАГЛУШКА ДЛЯ 2 товарища ---
@task
def load_data_to_dwh(dask_df):
    """Принимает данные от Dask и загружает их в PostgreSQL."""
    logger = get_run_logger()
    if dask_df is None:
        logger.error("Нет данных для загрузки.")
        return

    try:
        logger.info("Выполняю compute() и загружаю данные в Postgres...")
        final_pd_df = dask_df.compute()
        
        success = load_data_to_postgres(final_pd_df, table_name='wines')
        
        if success:
            logger.info("ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ В DWH!")
    except Exception as e:
        logger.error(f"Ошибка на этапе загрузки: {e}")


# --- ГЛАВНЫЙ FLOW, КОТОРЫЙ СОБИРАЕТ ВСЕ ВМЕСТЕ ---
@flow(name="Main ETL Flow", log_prints=True)
def main_etl_flow():
    s3_path = extract_data_to_minio()
    processed_data = transform_data_with_dask(s3_path)
    load_data_to_dwh(processed_data)

if __name__ == "__main__":
    # main_etl_flow.to_deployment(name="main-etl-deployment", work_pool_name="default-agent-pool")
    main_etl_flow()