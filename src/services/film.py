import json
import logging
from functools import lru_cache
from typing import List, Optional, Tuple

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.film import Film


logger = logging.getLogger(__name__)
FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


def es_to_film(source: dict) -> Film:
    return Film(
        uuid=source["id"],
        title=source.get("title"),
        imdb_rating=source.get("imdb_rating"),
        description=source.get("description"),
        genres=[{"uuid": g["id"], "name": g["name"]}
               for g in source.get("genres", [])],
        actors=[{"uuid": p["id"], "full_name": p["name"]}
                for p in source.get("actors", [])],
        writers=[{"uuid": p["id"], "full_name": p["name"]}
                 for p in source.get("writers", [])],
        directors=[{"uuid": p["id"], "full_name": p["name"]}
                   for p in source.get("directors", [])],
    )


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, film_id: str) -> Optional[Film]:
        film = await self._film_from_cache(film_id)
        if not film:
            film = await self._get_film_from_elastic(film_id)
            if not film:
                return None
            await self._put_film_to_cache(film)
        return film

    async def _get_film_from_elastic(self, film_id: str) -> Optional[Film]:
        try:
            doc = await self.elastic.get(index="movies", id=film_id)
            source = doc["_source"]
        except NotFoundError:
            return None
        return es_to_film(source)

    async def _film_from_cache(self, film_id: str) -> Optional[Film]:
        data = await self.redis.get(f"film:{film_id}")
        if not data:
            return None
        # pydantic предоставляет удобное API
        # для создания объекта моделей из json
        return Film.parse_raw(data)

    async def _put_film_to_cache(self, film: Film):
        # pydantic позволяет сериализовать модель в json
        await self.redis.set(
            f"film:{film.uuid}",
            film.json(),
            FILM_CACHE_EXPIRE_IN_SECONDS,
        )

    def _prepare_sort_and_pagination(
            self,
            sort_by: str,
            page_number: int,
            page_size: int,
    ) -> Tuple[str, str, int, int]:
        order = "desc" if sort_by.startswith("-") else "asc"
        sort_field = sort_by.lstrip("+-") or "imdb_rating"
        from_index = (page_number - 1) * page_size
        return order, sort_field, from_index, page_size

    async def _get_films_from_elastic_list(
            self,
            sort_by: str,
            page_number: int,
            page_size: int
    ) -> List[Film]:
        """
        Получает список фильмов из Elasticsearch с сортировкой и пагинацией.
        """

        order, sort_field, from_index, size_limit = self._prepare_sort_and_pagination(
            sort_by,
            page_number,
            page_size,
        )
        search_body = {
            "query": {"match_all": {}},
            "sort": [{sort_field: {"order": order}}],
            "from": from_index,
            "size": size_limit
        }
        try:
            response = await self.elastic.search(
                index="movies",
                body=search_body,
            )
            films_data = [hit["_source"] for hit in response["hits"]["hits"]]
            return [es_to_film(data) for data in films_data]
        except Exception as e:
            logger.error(f"Не удалось получить фильмы из Elasticsearch: {e}")
            return []

    async def _get_items_with_cache(
            self,
            cache_key: str,
            get_items_func,
            *args, **kwargs,
    ) -> List[Film]:
        cached_data = await self.redis.get(cache_key)
        if cached_data:
            return [Film.parse_obj(item) for item in json.loads(cached_data)]

        items = await get_items_func(*args, **kwargs)
        if items:
            await self.redis.set(
                cache_key,
                json.dumps([item.dict() for item in items]),
                FILM_CACHE_EXPIRE_IN_SECONDS,
            )
        return items

    async def get_films_list(
            self,
            sort_by: str,
            page_number: int,
            page_size: int
    ) -> List[Film]:
        cache_key = f"films:list:sort:{sort_by}:page:{page_number}:size:{page_size}"
        return await self._get_items_with_cache(
            cache_key,
            self._get_films_from_elastic_list,
            sort_by,
            page_number,
            page_size,
        )

    async def search_films(
            self,
            query: str,
            sort_by: str,
            page_number: int,
            page_size: int
    ) -> List[Film]:
        cache_key = f"films:search:query:{query}:sort:{sort_by}:page:{page_number}:size:{page_size}"
        return await self._get_items_with_cache(
            cache_key,
            self._search_films_from_elastic,
            query,
            sort_by,
            page_number,
            page_size,
        )

    async def _search_films_from_elastic(
            self,
            query: str,
            sort_by: str,
            page_number: int,
            page_size: int
    ) -> List[Film]:
        order, sort_field, from_index, size_limit = self._prepare_sort_and_pagination(
            sort_by,
            page_number,
            page_size,
        )

        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title", "description"],
                    "fuzziness": "AUTO"
                }
            },
            "sort": [{sort_field: {"order": order}}],
            "from": from_index,
            "size": size_limit
        }

        try:
            response = await self.elastic.search(
                index="movies",
                body=search_body,
            )
            films_data = [hit["_source"] for hit in response["hits"]["hits"]]
            return [es_to_film(data) for data in films_data]
        except Exception as e:
            logger.error(f"Не удалось получить фильмы из Elasticsearch: {e}")
            return []


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
