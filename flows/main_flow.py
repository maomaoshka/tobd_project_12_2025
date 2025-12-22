from prefect import task, flow
import requests 
import boto3 # для работы с MinIO
from botocore.exceptions import ClientError
from datetime import datetime
import pandas as pd
import os

# --- ЗАДАЧА №1: товарищ 1 ---
@task(retries=3, retry_delay_seconds=5)
def extract_data_to_minio():
    """
    Находит все CSV-файлы в локальной папке /data и загружает
    КАЖДЫЙ из них по отдельности в MinIO.
    """
    print("Начинаю извлечение данных из локальных CSV-файлов...")
    
    data_dir = '/app/data'

    try:
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
        if not csv_files:
            raise FileNotFoundError("В папке /app/data не найдено ни одного CSV-файла.")
        print(f"Обнаружено {len(csv_files)} CSV файлов для загрузки: {csv_files}")
    except FileNotFoundError as e:
        print(f"ОШИБКА: Папка {data_dir} не найдена. Проверьте volumes в docker-compose.yml")
        raise e

    minio_client = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
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
def transform_data_with_dask(s3_paths: list): # Теперь на вход приходит СПИСОК
    """Заглушка: обрабатывает данные с помощью Dask."""
    print(f"Начинаю обработку {len(s3_paths)} файлов...")
    for path in s3_paths:
        print(f"  - Обрабатывается файл: {path}")
    print("... здесь будет логика Dask от Инженера по обработке ...")
    processed_data = "ТУТ БУДУТ ЧИСТЫЕ ДАННЫЕ ПОСЛЕ ОБРАБОТКИ ВСЕХ ФАЙЛОВ"
    return processed_data


# --- ЗАДАЧА №3: ЗАГЛУШКА ДЛЯ 2 товарища ---
@task
def load_data_to_dwh(data):
    """Заглушка: загружает данные в DWH."""
    print("Начинаю загрузку данных в DWH...")
    print(f"Получены данные для загрузки: {data}")
    print("... здесь будет логика загрузки в PostgreSQL от Инженера данных ...")
    print("Данные успешно загружены в DWH!")


# --- ГЛАВНЫЙ FLOW, КОТОРЫЙ СОБИРАЕТ ВСЕ ВМЕСТЕ ---
@flow(name="Main ETL Flow", log_prints=True)
def main_etl_flow():
    s3_path = extract_data_to_minio()
    processed_data = transform_data_with_dask(s3_path)
    load_data_to_dwh(processed_data)

if __name__ == "__main__":
    main_etl_flow.to_deployment(name="main-etl-deployment", work_pool_name="default-agent-pool")