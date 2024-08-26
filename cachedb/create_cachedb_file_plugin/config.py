from pydantic import BaseModel
from typing import Any


class CreateDuckdbDatabaseFileType(BaseModel):
    databaseCode: str
    schemaName: str

    @property
    def use_cache_db(self) -> str:
        return True

class CreateDuckdbDatabaseFileModules(BaseModel):
    # TODO: TBD disscuss a better way to handle dynamic imports
    utils_types: Any
    dao_DBDao: Any
