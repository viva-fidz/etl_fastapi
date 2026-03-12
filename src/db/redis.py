from typing import Optional
from redis.asyncio import Redis, ConnectionPool
from core.config import settings
import logging

logger = logging.getLogger(__name__)

redis: Optional[Redis] = None
pool: Optional[ConnectionPool] = None


async def init_redis():
    global redis, pool
    pool = ConnectionPool(
        host=settings.redis_host,
        port=settings.redis_port,
        max_connections=settings.redis_max_connections,
        decode_responses=True,
        socket_connect_timeout=settings.redis_connect_timeout,
        socket_keepalive=True,
    )
    redis = Redis(connection_pool=pool)

    try:
        await redis.ping()
        logger.info("Redis успешно подключен")
    except Exception as e:
        logger.error(f"Не удалось подключиться к Redis: {e}")
        raise


async def close_redis():
    if redis:
        await redis.close()
    if pool:
        await pool.disconnect()
        logger.info("Пул соединений Redis закрыт")


def get_redis() -> Redis:
    if redis is None:
        raise RuntimeError("Redis не был инициализирован")
    return redis


async def get_redis_dependency() -> Redis:
    """Dependency для использования в FastAPI."""

    return get_redis()
