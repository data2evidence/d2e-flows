from pydantic import BaseModel
from typing import Optional, List, Dict


class DqdBaseOptionsType(BaseModel):
    schemaName: str
    databaseCode: str
    cdmVersionNumber: str
    vocabSchemaName: str
    releaseDate: str


class DqdOptionsType(DqdBaseOptionsType):
    cohortDefinitionId: Optional[str]
    checkNames: Optional[List[str]]
    cohortDatabaseSchema: Optional[str]
    cohortTableName: Optional[str]
    
    @property
    def use_cache_db(self) -> str:
        return False