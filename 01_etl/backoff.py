from functools import wraps

import psycopg2
import elasticsearch

from time import sleep

from logger import log_to_file


def backoff(message_before: str, message_after:str,
            start_sleep_time: float=0.1, factor: int=2, border_sleep_time: int=10):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост
    времени повтора (factor) до граничного времени ожидания
    (border_sleep_time)

    @param: start_sleep_time
    @param: factor
    @param: border_sleep_time
    @param: message_before Сообщение для лога ДО выполнения
    @param: message_after Сообщение для лога ПОСЛЕ выполнения
    @rtype: object
    @return: Возвращает декоратор
    """
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            counter = 1

            while True:
                try:
                    log_to_file(message_before)
                    connection = func(*args, **kwargs)
                    log_to_file(message_after)
                    return connection
                except (psycopg2.OperationalError,
                        elasticsearch.ConnectionError) as error:
                    # обработка ошибки подключения к Postgre, ElasticSearch
                    log_to_file(error)

                sleep_time = min(start_sleep_time * factor ** counter, border_sleep_time)
                counter += 1
                sleep(sleep_time)

        return inner

    return func_wrapper