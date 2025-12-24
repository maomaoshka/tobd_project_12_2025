import os
import pandas as pd
from sqlalchemy import create_engine
from utils.logging_config import get_logger

logger = get_logger(__name__)

def load_data_to_postgres(df: pd.DataFrame, table_name: str = 'wines'):
    """
    Загружает Pandas DataFrame в базу данных PostgreSQL.
    Использует переменные окружения для гибкости настройки.
    """
    
    # 1. Получаем настройки подключения из окружения (или берем значения по умолчанию)
    # На твоем ПК автоматически сработает 'localhost', в Docker - 'postgres'
    db_host = os.getenv('POSTGRES_HOST', 'localhost')
    db_port = os.getenv('POSTGRES_PORT', '5432')
    db_user = os.getenv('POSTGRES_USER', 'postgres')
    db_pass = os.getenv('POSTGRES_PASSWORD', 'password')
    db_name = os.getenv('POSTGRES_DB', 'project_db')

    # 2. Формируем строку подключения (URL)
    connection_url = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    try:
        # 3. Создаем движок SQLAlchemy
        # Параметр client_encoding принудительно ставит UTF-8 для общения с базой
        engine = create_engine(
            connection_url, 
            client_encoding='utf8'
        )

        logger.info(f"Начинаю загрузку {len(df)} строк в таблицу '{table_name}' на {db_host}...")

        # 4. Загружаем данные
        # if_exists='append' — добавляем в существующую таблицу
        # index=False — не создаем отдельный столбец для индексов pandas
        # method='multi' — ускоряет вставку нескольких строк сразу
        df.to_sql(
            table_name, 
            engine, 
            if_exists='append', 
            index=False, 
            method='multi'
        )

        logger.info("✅ Загрузка успешно завершена!")
        return True

    except Exception as e:
        # repr(e) используется, чтобы избежать ошибок кодировки при выводе русских системных сообщений
        logger.error(f"❌ Ошибка при загрузке данных в БД: {repr(e)}")
        return False

# Блок для локального тестирования
# if __name__ == "__main__":
#     print("--- Запуск теста функции загрузки ---")
    
#     # Создаем пример данных (такой же структуры, как в init_db.sql)
#     test_data = [
#         {
#             'wine_title': 'Test Wine 1', 
#             'country': 'Italy', 
#             'rating': 4.5, 
#             'price': 1200.50, 
#             'wine_type': 'red'
#         },
#         {
#             'wine_title': 'Test Wine 2', 
#             'country': 'France', 
#             'rating': 3.8, 
#             'price': 2500.00, 
#             'wine_type': 'white'
#         }
#     ]
#     df_test = pd.DataFrame(test_data)
    
#     # Пытаемся загрузить данные
#     success = load_data_to_postgres(df_test)
    
#     if success:
#         print("Тест пройден успешно!")
#     else:
#         print("Тест провален. Проверьте запущен ли Docker и верны ли параметры подключения.")