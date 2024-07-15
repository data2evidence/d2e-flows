from pydantic import BaseModel
from typing import List
        
class StrategusAnalysisType(BaseModel):
    sharedResources: List
    moduleSpecifications: List


class StrategusOptionsType(BaseModel):
    workSchema: str
    cdmSchema: str
    databaseCode: str
    vocabSchemaName: str