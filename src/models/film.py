from typing import Optional, List
from pydantic import BaseModel, Field


class Genre(BaseModel):
    uuid: str
    name: str


class Person(BaseModel):
    uuid: str
    full_name: str


class Film(BaseModel):
    uuid: str
    title: str
    imdb_rating: Optional[float] = None
    description: Optional[str] = None
    genres: List[Genre] = Field(default_factory=list)
    actors: List[Person] = Field(default_factory=list)
    writers: List[Person] = Field(default_factory=list)
    directors: List[Person] = Field(default_factory=list)
