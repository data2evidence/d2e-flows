from pydantic import BaseModel

class CohortJsonType(BaseModel):
    id: int
    name: str
    createdDate: int
    modifiedDate: int
    hasWriteAccess: bool
    tags: list
    expressionType: str
    expression: dict

class CohortGeneratorOptionsType(BaseModel):
    databaseCode: str
    schemaName: str
    vocabSchemaName: str
    cohortJson: CohortJsonType
    datasetId: str
    description: str
    owner: str
    token: str
    
    @property
    def use_cache_db(self) -> str:
        return False