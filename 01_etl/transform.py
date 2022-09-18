from dataclasses import dataclass

from data_classes import Filmwork, Person


@dataclass()
class TransformerToES:

    def transform_data(self, data):
        """
        Преобразование данных из Postgre
        для записи в ES

        :param data: данные, полученные SQL-запросом
        :return: преобразованные данные
        """

        for filmwork in data:
            id, title, description, rating, persons, genres, updated_at = filmwork
            
            filmwork_ = Filmwork(
                id=id,
                imdb_rating=rating,
                genre=genres,
                title=title,
                description=description,
                director=[person['person_name'] for person in persons
                          if person['person_role'] == 'director'],
                actors_names=[person['person_name'] for person in persons
                              if person['person_role'] == 'actor'],
                writers_names=[person['person_name'] for person in persons
                               if person['person_role'] == 'writer'],
                actors=[Person(id=person['person_id'], name=person['person_name'])
                        for person in persons
                        if person['person_role'] == 'actor'],
                writers=[Person(id=person['person_id'], name=person['person_name'])
                         for person in persons
                         if person['person_role'] == 'writer']
            )

            yield filmwork_, updated_at

