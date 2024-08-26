from pydantic import BaseModel
from typing import Any, Optional


class CreateDuckdbDatabaseFileType(BaseModel):
    databaseCode: str
    schemaName: str
    
    # Flag used for cdw-config to create empty duckdb database file for validation
    # When this flag is set to True, it will also save the duckdb database file into a separate volume only for cdw-svc
    createForCdwConfigValidation: Optional[bool] = False

    @property
    def use_cache_db(self) -> str:
        return True

class CreateDuckdbDatabaseFileModules(BaseModel):
    # TODO: TBD disscuss a better way to handle dynamic imports
    utils_types: Any
    dao_DBDao: Any
