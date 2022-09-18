from pydantic import BaseModel


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
