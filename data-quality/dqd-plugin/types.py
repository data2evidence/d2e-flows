from pydantic import BaseModel
from typing import Optional, List, Dict


class dqdBaseOptionsType(BaseModel):
    schemaName: str
    databaseCode: str
    cdmVersionNumber: str
    vocabSchemaName: str
    releaseDate: str


class dqdOptionsType(dqdBaseOptionsType):
    cohortDefinitionId: Optional[str]
    checkNames: Optional[List[str]]