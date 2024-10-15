from pydantic import BaseModel
from typing import Optional


class CreateFhirDataModelOptions(BaseModel):
    schema_name: str
    database_code: str

    @property
    def use_cache_db(self) -> str:
        return True