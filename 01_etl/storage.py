import json

from typing import Optional, Any


class JsonFileStorage:
    """
    Класс для чтения/записи из/в файл состояния
    """

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def retrieve_state(self):
        # чтение из файла
        try:
            with open(self.file_path, "r") as readfile:
                state = json.load(readfile)
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            state = {}
        return state

    def save_state(self, state: dict):
        # запись в файл
        with open(self.file_path, "w") as writefile:
            json.dump(state, writefile)


class State:
    """
    Класс для хранения состояния при работе с данными
    """

    def __init__(self, storage: JsonFileStorage):
        self.storage = storage
        self.state = storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        # Установка состояния для определённого ключа
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        # Получение состояния по определённому ключу
        return self.state.get(key)
