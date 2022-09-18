from pydantic import BaseModel
from typing import List, Optional

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
    genre: Optional[List[str]]
    title: str
    description: Optional[str]
    director: Optional[List[str]]
    actors_names: Optional[List[str]]
    writers_names: Optional[List[str]]
    actors: Optional[List[Person]]
    writers: Optional[List[Person]]


class FilmworkStorage:
    """
    Класс-хранилице кинопроизведений
    """
    storage: List[Optional[Filmwork]] = []

    def append(self, filmwork: Filmwork) -> None:
        """
        Добавляет кинопроизведение в хранилище

        :param filmwork: кинопроизведение
        """
        self.storage.append(filmwork)

    def clear(self) -> None:
        """
        Очищает хранилище
        """
        self.storage.clear()
