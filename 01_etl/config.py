import os

from pydantic import BaseModel
from dotenv import load_dotenv


# грузим переменные окружения
load_dotenv()

SLEEP_TIME = 8
BATCH_SIZE = 150

es_settings = {
    'host': os.environ.get('ES_HOST'),
    'port': os.environ.get('ES_PORT')
}

pg_settings = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': int(os.environ.get('DB_PORT')),
    'options': os.environ.get('DB_OPTIONS'),
}

class PostgreSettings(BaseModel):
    host: str
    dbname: str
    user: str
    password: str
    port: int
    options: str


class ESSettings(BaseModel):
    host: str
    port: int
