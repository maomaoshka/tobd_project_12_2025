import dask.dataframe as dd
from typing import Dict, Any
from utils.logging_config import get_logger

logger = get_logger(__name__)

class MinIODataProcessor:
    """Класс для обработки данных из MinIO с помощью Dask."""
    
    def __init__(self, storage_options: Dict[str, Any]):
        self.storage_options = storage_options
        logger.info("MinIODataProcessor инициализирован")

    def read_from_minio(self, s3_path: str, **read_kwargs) -> dd.DataFrame:
        logger.info(f"Чтение данных из {s3_path}")
        
        default_kwargs = {
            "storage_options": self.storage_options,
            "dtype": "object",
            "on_bad_lines": "warn",
            "low_memory": False,
        }
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

            ddf["Year"] = ddf["Year"].astype("int64")
            ddf["Price"] = ddf["Price"].astype("float64")
            ddf["Rating"] = ddf["Rating"].astype("float64")
            ddf["NumberOfRatings"] = ddf["NumberOfRatings"].astype("int64")
            ddf["WineType"] = wine

            dataframes.append(ddf)

        return dd.concat(dataframes, axis=0, ignore_index=True)


def get_minio_storage_options(
    endpoint: str = "localhost:19090",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin"
) -> Dict[str, Any]:
    
    return {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {
            "endpoint_url": f"http://{endpoint}"
        },
        "config_kwargs": {
            "signature_version": "s3v4"
        }
    }
