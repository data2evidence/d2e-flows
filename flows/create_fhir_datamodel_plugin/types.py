from pydantic import BaseModel
from typing import Optional


class CreateFhirDataModelOptions(BaseModel):
    schema_name: Optional[str]
    database_code: Optional[str]
    token: Optional[str] = ""

    @property
    def use_cache_db(self) -> str:
        return True