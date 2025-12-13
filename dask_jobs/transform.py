import dask.dataframe as dd
import pandas as pd
import logging
from typing import Optional, Dict, Any
from datetime import datetime
import numpy as np
from minio import Minio

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MinIODataProcessor:
    """Класс для обработки данных из MinIO с помощью Dask."""
    
    def __init__(self, storage_options: Dict[str, Any]):
        """
        Инициализация процессора данных.
        
        Args:
            storage_options: Настройки для подключения к MinIO/S3
        """
        self.storage_options = storage_options
        logger.info("MinIODataProcessor инициализирован")

    def read_from_minio(self, s3_path: str, **read_kwargs) -> dd.DataFrame:
        """
        Чтение данных из MinIO.
        
        Args:
            s3_path: Путь к файлу в MinIO (s3://bucket/path/file.csv)
            **read_kwargs: Дополнительные аргументы для dd.read_csv
            
        Returns:
            Dask DataFrame с данными
        """
        logger.info(f"Чтение данных из {s3_path}")
        
        # Базовые настройки чтения
        default_kwargs = {
            'storage_options': self.storage_options,
            'dtype': 'object',  # Читаем все как строки для гибкости
            'on_bad_lines': 'warn',  # Предупреждаем о плохих строках
            'low_memory': False,
        }
        
        # Объединяем с пользовательскими настройками
        read_kwargs = {**default_kwargs, **read_kwargs}
        
        try:
            ddf = dd.read_csv(s3_path, na_values=["N.V."], **read_kwargs)
            logger.info(f"Успешно прочитано. Форма данных: {ddf.shape}")
            return ddf
        except Exception as e:
            logger.error(f"Ошибка чтения данных из {s3_path}: {e}")
            raise

    def transform(self, s3_path: str):
        wine_types = ["Red", "White", "Rose", "Sparkling"]
        dataframes = []

        for wine in wine_types:
            ddf = self.read_from_minio(f"{s3_path}/{wine}.csv")
            ddf = ddf.dropna()

            ddf['Year'] = ddf['Year'].astype('int64')
            # print(ddf['Year'].compute())
            ddf['Price'] = ddf['Price'].astype('float64')
            ddf['Rating'] = ddf['Rating'].astype('float64')
            ddf['NumberOfRatings'] = ddf['NumberOfRatings'].astype('int64')
            ddf['WineType'] = wine
            dataframes.append(ddf)
        return dd.concat(dataframes, axis=0, ignore_index=True)


def get_minio_storage_options(
    endpoint: str = "localhost:19090",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin"
) -> Dict[str, Any]:
    """
    Создание настроек подключения к MinIO.
    
    Args:
        endpoint: URL MinIO сервера
        access_key: Access key
        secret_key: Secret key
        region: Регион
        
    Returns:
        Словарь с настройками storage_options
    """
    return {
        'key': access_key,
        'secret': secret_key,
        'client_kwargs': {
            'endpoint_url': f'http://{endpoint}'
        },
        'config_kwargs': {
            'signature_version': 's3v4'
        }
    }


if __name__ == "__main__":
    """
    Пример использования скрипта.
    """
    # Настройки подключения к MinIO (из вашего docker-compose)
    STORAGE_OPTIONS = get_minio_storage_options(
        endpoint="localhost:19090",
        access_key="minioadmin",
        secret_key="minioadmin"
    )
    
    # Пути к файлам
    INPUT_PATH = "s3://raw-data/myfile.csv"  # Или любой другой файл в вашем MinIO
    OUTPUT_PATH = "s3://processed-data/cleaned_data.parquet"
    
    # Запуск ETL процесса
    try:
        logger.info("Запуск основного скрипта...")
        # Выполняем ETL
        # cleaned_ddf = transform_and_clean(
        #     input_path_s3=INPUT_PATH,
        #     storage_options=STORAGE_OPTIONS,
        #     output_path_s3=OUTPUT_PATH
        # )
        processor = MinIODataProcessor(STORAGE_OPTIONS)
        ddf = processor.transform("s3://vivino/archive (1)")
        


        # Выводим информацию о результате
        print("\n" + "="*60)
        print("РЕЗУЛЬТАТЫ ОБРАБОТКИ:")
        print(len(ddf))
        print(f"Колонки: {list(ddf.columns)}")
        # print(f"{ddf.compute()}")
        print(ddf.info)

        
    except Exception as e:
        logger.error(f"Ошибка выполнения: {e}")