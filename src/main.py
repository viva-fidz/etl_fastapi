import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from api.v1 import films
from db import elastic, redis


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Запускаем сервисы Redis и ElasticSearch")

    try:
        await redis.init_redis()
        await elastic.init_elastic()

        yield

    except Exception as e:
        logger.error(f"Ошибка при запуске сервисов: {e}")
        # Если инициализация не удалась, делаем явный выход
        # для предотвращения запуска приложения с некорректной инициализацией
        import sys
        sys.exit(1)

    finally:
        # Shutdown: очистка ресурсов
        logger.info("Завершаем работу сервисов...")
        await redis.close_redis()
        await elastic.close_elastic()


app = FastAPI(
    title="Read-only API для онлайн-кинотеатра",
    description="Информация о фильмах, жанрах и людях, "
                "участвовавших в создании произведения",
    version="1.0.0",
    docs_url="/api/openapi",
    openapi_url="/api/openapi.json",
    default_response_class=ORJSONResponse,
    lifespan=lifespan,
)

app.include_router(films.router, prefix="/api/v1/films")
# app.include_router(auth.router, prefix="/api/v1/auth")

@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "ok"}
