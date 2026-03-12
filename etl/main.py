import sys, os
import logging
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from etl_components.extract import PostgresExtractor
from etl_components.load import ElasticsearchLoader
from etl_components.transform import DataTransform

from state import JsonFileStorage, State


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    db_name: str = Field(alias="POSTGRES_DB")
    db_user: str = Field(alias="POSTGRES_USER")
    db_password: str = Field(alias="POSTGRES_PASSWORD")
    db_host: str = Field(alias="DB_HOST", default="localhost")
    db_port: int = Field(alias="DB_PORT", default=5432)
    es_host: str = Field(alias="ES_HOST", default="localhost")
    es_port: int = Field(alias="ES_PORT", default=9200)
    state_file: str = "state.json"
    sleep_interval: int = Field(alias="SLEEP_INTERVAL", default=60)

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


def main():
    settings = Settings()

    connection_params = {
        "host": settings.db_host,
        "port": settings.db_port,
        "dbname": settings.db_name,
        "user": settings.db_user,
        "password": settings.db_password,
    }
    es_host = settings.es_host
    es_port = settings.es_port

    storage = JsonFileStorage('state.json')
    state = State(storage)
    extractor = PostgresExtractor(connection_params, state, batch_size=100)
    transformer = DataTransform()
    elasticsearch_loader = ElasticsearchLoader(host=es_host, port=es_port)

    try:
        logger.info("Начинаем процесс ETL в цикле")
        while True:
            logger.info("Запускаем очередной проход ETL")
            for batch in extractor.extract_incremental():
                logger.info(f"Обрабатываем пачку из {len(batch)} записей")
                transformed_data = transformer.transform_batch(batch)
                result = elasticsearch_loader.load(transformed_data)
                logger.info(f"Загружено {result['indexed']} документов")
                if result['errors']:
                    logger.error(f"Ошибки при загрузке: {result['errors']}")

            logger.info("Проход ETL завершен")
            time.sleep(settings.sleep_interval)

    except Exception as e:
        logger.error(f"Ошибка в процессе ETL: {e}")
        raise


if __name__ == "__main__":
    main()
