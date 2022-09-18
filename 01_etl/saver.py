import os
import json
import datetime

from config import ESSettings
from backoff import backoff
from data_classes import FilmworkStorage, Filmwork
from logger import log_to_file
from storage import JsonFileStorage, State

from elasticsearch import Elasticsearch, helpers, ConnectionError

from dotenv import load_dotenv

from typing import Iterator, Optional, List


# грузим переменные окружения
load_dotenv()

es_settings = {
    'host': os.environ.get('ES_HOST'),
    'port': os.environ.get('ES_PORT')
}


class ESSaver:
    host_settings: ESSettings
    connection: Elasticsearch
    index_name: str
    config: str

    def __init__(self):
        self.host_settings = ESSettings(**es_settings)
        self.connection = self.get_connection()
        self.index_name = 'movies'
        self.index_config = 'es_schema.json'

        if not self.check_index():
            self.create_index()

    @backoff()
    def get_connection(self) -> Elasticsearch:
        """
        Получение соединения ElasticSearch

        :return: соединение ElasticSearch
        """
        connection = Elasticsearch(f"http://{self.host_settings.host}:"
                                   f"{self.host_settings.port}")
        return connection

    def check_index(self) -> None:
        """
        Проверка наличия созданного индекса в ES
        """
        return self.connection.indices.exists(index=self.index_name)

    def create_index(self) -> None:
        """
        Создание индекса с заданными настройками
        """
        with open(self.index_config, 'r') as index_config:
            index_settings = json.load(index_config)

        self.connection.indices.create(
            index=self.index_name,
            body=index_settings
        )

    def save_to_es(self, data: Optional[Iterator]) -> None:
        """
        Сохранение данных в ElasticSearch

        :param data: трансформированные данные из PostgreSQL
        """
        state = State(JsonFileStorage('state.json'))

        try:
            filmworks = (
                {
                    "_index": "movies",
                    "_id": filmwork.id,
                    "_source": filmwork.json()
                }
                for filmwork in data
            )

            filmworks, errors = helpers.bulk(self.connection, filmworks)
            log_to_file(f'Загружено {filmworks} записей, ошибок - {errors}')

            state.set_state('last_load_date',
                            datetime.datetime.isoformat(datetime.datetime.now()))

        except ConnectionError as error:
            log_to_file(f'При сохранении возникла ошибка - {error}')
