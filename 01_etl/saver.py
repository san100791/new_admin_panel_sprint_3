import json
import datetime

from config import ESSettings, es_settings
from backoff import backoff
from data_classes import Filmwork
from logger import log_to_file
from storage import JsonFileStorage, State

from elasticsearch import Elasticsearch, helpers, ConnectionError

from typing import Iterator, Optional, List


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

    @backoff(message_before="Try to connect to ElasticSearch",
             message_after="Successful connect to ElasticSearch")
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
            success = 0
            errors = 0
            last_save_date = None

            if data:
                for filmwork in data:
                    record = self.connection.index(index="movies",
                                          id=filmwork[0].id,
                                          body=filmwork[0].json())

                    if record['result'] == 'updated' or 'created':
                        success += 1
                        last_save_date = filmwork[1]
                    else:
                        errors += 1
                        log_to_file(f'Error in save filmwork with id {filmwork[0].id}')
                if errors:
                    log_to_file(f'{errors} errors in save')
                else:
                    log_to_file(f'Successful save to ES')

                if last_save_date:
                    state.set_state('last_load_date',
                                    datetime.datetime.isoformat(last_save_date))

            else:
                log_to_file('Nothing to save to ES')

        except ConnectionError as error:
            log_to_file(f'Error in save to ES - {error}')
