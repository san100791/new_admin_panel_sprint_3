from functools import wraps

import psycopg2
import elasticsearch

from time import sleep

from logger import log_to_file


def backoff(start_sleep_time: float=0.1, factor: int=2, border_sleep_time: int=10):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост
    времени повтора (factor) до граничного времени ожидания
    (border_sleep_time)

    @param: start_sleep_time
    @param: factor
    @param: border_sleep_time
    @rtype: object
    @return: Возвращает декоратор
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            counter = 1

            while True:
                try:
                    log_to_file('Попытка подключения к БД...')
                    connection = func(*args, **kwargs)
                    log_to_file('Успешное подключение к БД')
                    return connection
                except psycopg2.OperationalError as pg_error:
                    # обработка ошибки подключения к Postgre
                    log_to_file(pg_error)
                except elasticsearch.ConnectionError as es_error:
                    # обработка ошибки подключения к ElasticSearch
                    log_to_file(es_error)

                sleep_time = min(start_sleep_time * factor ** counter, border_sleep_time)
                counter += 1
                sleep(sleep_time)

        return inner

    return func_wrapper