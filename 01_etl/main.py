from loader import PostgreLoader
from transform import TransformerToES
from saver import ESSaver
from logger import log_to_file
from config import SLEEP_TIME

from time import sleep


def start_etl() -> None:
    """
    Запуск ETL-процесса
    """
    pg_loader = PostgreLoader()
    pg_to_es_transformer = TransformerToES()
    es_saver = ESSaver()

    log_to_file('Запуск ETL-процесса')

    while True:
        filmwork_data = pg_loader.fetch_data()
        transformed_data = \
            pg_to_es_transformer.transform_data(filmwork_data)
        if transformed_data:
            es_saver.save_to_es(transformed_data)

        sleep(SLEEP_TIME)


if __name__ == '__main__':
    start_etl()
