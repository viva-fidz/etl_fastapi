import logging
from typing import Optional

from elasticsearch import AsyncElasticsearch

from core.config import settings


logger = logging.getLogger(__name__)

es: Optional[AsyncElasticsearch] = None


async def init_elastic():
    global es
    es = AsyncElasticsearch(
        hosts=[f"http://{settings.elastic_host}:{settings.elastic_port}"],
        maxsize=settings.elastic_max_connections,
        retry_on_timeout=True,
        request_timeout=settings.elastic_timeout,
        http_compress=True,
    )

    try:
        await es.ping()
        logger.info("Elasticsearch успешно подключен")
    except Exception as e:
        logger.error(f"Не удалось подключиться к Elasticsearch: {e}")
        raise


async def close_elastic():
    if es:
        await es.close()
        logger.info("Подключение к Elasticsearch закрыто")


def get_elastic() -> AsyncElasticsearch:
    if es is None:
        raise RuntimeError("Elasticsearch не был инициализирован")
    return es


async def get_elastic_dependency() -> AsyncElasticsearch:
    """Dependency для использования в FastAPI."""

    return get_elastic()
