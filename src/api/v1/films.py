from http import HTTPStatus
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel

from models.film import Film
from services.film import FilmService, get_film_service


router = APIRouter()


class FilmListItem(BaseModel):
    uuid: str
    title: str
    imdb_rating: Optional[float] = None


class FilmSearchListItem(BaseModel):
    uuid: str
    title: str
    imdb_rating: Optional[float] = None


@router.get(
    "/search",
    response_model=List[FilmSearchListItem],
    summary="Поиск фильмов",
    description="Поиск фильмов по названию или описанию с пагинацией и сортировкой",
    response_description="Список найденных фильмов",
    tags=["Полнотекстовый поиск"],
)
async def search_films(
        query: str = Query(..., description="Поисковый запрос"),
        sort: str = Query(default="-imdb_rating",
                          description="Поле для сортировки"),
        page_size: int = Query(
            default=50, ge=1, le=100,
            description="Количество элементов на странице",
        ),
        page_number: int = Query(
            default=1, ge=1,
            description="Номер страницы",
        ),
        film_service: FilmService = Depends(get_film_service)
) -> List[FilmSearchListItem]:
    films = await film_service.search_films(
        query=query,
        sort_by=sort,
        page_number=page_number,
        page_size=page_size
    )
    if not films:
        return []
    return [
        FilmSearchListItem(
            uuid=film.uuid,
            title=film.title,
            imdb_rating=film.imdb_rating
        )
        for film in films
    ]


@router.get(
    "/{film_id}",
    response_model=Film,
    summary="Детали фильма",
    description="Получить полную информацию о фильме по UUID",
    response_description="Полная информация о фильме",
    tags=["Фильм"],
)
async def film_details(
        film_id: str = Path(..., description="UUID фильма"),
        film_service: FilmService = Depends(get_film_service)
) -> Film:
    film = await film_service.get_by_id(film_id)
    if not film:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='film not found',
        )
    return film


@router.get(
    "/",
    response_model=List[FilmListItem],
    summary="Все фильмы",
    description="Получить список всех фильмов с пагинацией и фильтрацией",
    response_description="Список отсортированных фильмов",
    tags=["Фильмы"],
)
async def films_list(
    sort: str = Query(
        default="-imdb_rating",
        description="Поле для сортировки",
    ),
    page_size: int = Query(
        default=50, ge=1, le=100,
        description="Количество элементов на странице",
    ),
    page_number: int = Query(
        default=1, ge=1,
        description="Номер страницы",
    ),
    film_service: FilmService = Depends(get_film_service)
) -> List[FilmListItem]:
    films = await film_service.get_films_list(
        sort_by=sort,
        page_number=page_number,
        page_size=page_size
    )
    if not films:
        return []
    return [
        FilmListItem(
            uuid=film.uuid,
            title=film.title,
            imdb_rating=film.imdb_rating
        )
        for film in films
    ]
