from typing import List, Optional

from pydantic import BaseModel


class PersonName(BaseModel):
    id: str
    name: str


class Genre(BaseModel):
    id: str
    name: str


class FilmES(BaseModel):
    id: str
    imdb_rating: Optional[float] = None
    genres: List[Genre] = []
    title: str
    description: Optional[str] = None

    directors_names: List[str] = []
    actors_names: List[str] = []
    writers_names: List[str] = []

    directors: List[PersonName] = []
    actors: List[PersonName] = []
    writers: List[PersonName] = []
