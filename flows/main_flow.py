from prefect import task, flow
import requests 
import boto3 # для работы с MinIO
from botocore.exceptions import ClientError
from datetime import datetime

# --- ЗАДАЧА №1: товарищ 1 ---
@task(retries=3, retry_delay_seconds=10)
def extract_data_to_minio():
    """Извлекает данные по API и сохраняет их в MinIO."""
    print("Начинаю извлечение данных...")
    
    # 1. Подключаемся к источнику (пример: API Московской биржи)
    url = "https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json"
    response = requests.get(url)
    response.raise_for_status() # Проверяем, что запрос успешный
    data = response.text # Получаем данные как текст

    # 2. Подключаемся к MinIO
    # Эти данные берутся из вашего docker-compose.yml
    minio_client = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )

    # 3. Создаем "ведро" (bucket), если его нет
    bucket_name = "raw-data"
    try:
        minio_client.head_bucket(Bucket=bucket_name)
    except ClientError:
        print(f"Bucket {bucket_name} не найден. Создаю новый.")
        minio_client.create_bucket(Bucket=bucket_name)

    # 4. Сохраняем данные в файл в MinIO
    # Генерируем уникальное имя файла с датой и временем
    file_name = f"moex_shares_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Загружаем данные
    minio_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=data.encode('utf-8') # Кодируем текстовые данные в байты
    )
    print(f"Данные успешно сохранены в MinIO: {bucket_name}/{file_name}")
    
    # Возвращаем путь к файлу для следующей задачи
    return f"s3://{bucket_name}/{file_name}"


# --- ЗАДАЧА №2: ЗАГЛУШКА ДЛЯ 3 товарища ---
@task
def transform_data_with_dask(s3_path: str):
    """Заглушка: обрабатывает данные с помощью Dask."""
    print(f"Начинаю обработку файла: {s3_path}")
    print("... здесь будет логика Dask от Инженера по обработке ...")
    # В будущем эта задача вернет обработанный DataFrame
    processed_data = "ТУТ БУДУТ ЧИСТЫЕ ДАННЫЕ"
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