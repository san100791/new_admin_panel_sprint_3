import datetime
import os

from typing import Union, List, Iterator

from time import sleep

import psycopg2
from psycopg2.extensions import connection as pg_connection
from psycopg2.extras import DictCursor

from dataclasses import dataclass

from config import PostgreSettings
from backoff import backoff
from storage import JsonFileStorage, State
from sql import query_filmfork_ids
from logger import log_to_file

from dotenv import load_dotenv


# грузим переменные окружения
load_dotenv()

dsl = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': int(os.environ.get('DB_PORT')),
    'options': os.environ.get('DB_OPTIONS'),
}


class PostgreLoader:
    settings: PostgreSettings
    connection: pg_connection
    last_check_date: datetime.datetime
    batch_size: int

    def __init__(self):
        self.settings = PostgreSettings(**dsl)
        self.connection = self.get_connection()
        self.batch_size = 50

    @backoff()
    def get_connection(self) -> pg_connection:
        """
        Получение соединения PostgreSQL

        :return: соединение PostgreSQL
        """
        with psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
            return pg_conn

    def fetch_data(self) -> Union[List, Iterator]:
        """
        Осуществляет выгрузку фильмов из Postgre,
        обновленных не ранее даты, указанной в файле
        состояния

        :return: Совокупность данных по нужным фильмам
        """
        log_to_file('Попытка загрузки данных из PostgreSQL')

        state = State(JsonFileStorage('state.json'))
        if not state.get_state('last_load_date'):
            state.set_state('last_load_date',
                            datetime.datetime.isoformat(datetime.datetime.min))

        try:
            if self.connection.closed:
                self.connection = self.get_connection()
            cursor = self.connection.cursor()
            cursor.execute(query_filmfork_ids,
                           (state.get_state('last_load_date'),)
                           )

            while True:
                part_of_data = cursor.fetchmany(self.batch_size)
                if not part_of_data:
                    log_to_file('Данные для загрузки отсутствуют')
                    return []
                else:
                    log_to_file('Загрузка данных...')
                    for row in part_of_data:
                        yield row

        except psycopg2.OperationalError as e:
            log_to_file(f'Ошибка загрузки данных: {e}')
