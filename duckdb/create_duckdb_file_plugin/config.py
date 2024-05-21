from pydantic import BaseModel
from typing import Any


class CreateDuckdbDatabaseFileType(BaseModel):
    databaseCode: str
    schemaName: str


class CreateDuckdbDatabaseFileModules(BaseModel):
    # TODO: TBD disscuss a better way to handle dynamic imports
    utils_types: Any
    alpconnection_dbutils: Any
    dao_DBDao: Any
