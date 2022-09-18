from pydantic import BaseModel
from typing import Optional

import uuid


class Person(BaseModel):
    """
    Pydantic-класс персоны
    """
    id: uuid.UUID
    name: str


class Filmwork(BaseModel):
    """
    Pydantic-класс кинопроизведения
    """
    id: uuid.UUID
    imdb_rating: Optional[float]
    genre: Optional[list[str]]
    title: str
    description: Optional[str]
    director: Optional[list[str]]
    actors_names: Optional[list[str]]
    writers_names: Optional[list[str]]
    actors: Optional[list[Person]]
    writers: Optional[list[Person]]

