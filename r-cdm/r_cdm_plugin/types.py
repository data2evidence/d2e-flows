from pydantic import BaseModel

class rCDMOptionsType(BaseModel):
    databaseCode: str
    schemaName: str
    cdmVersion: str