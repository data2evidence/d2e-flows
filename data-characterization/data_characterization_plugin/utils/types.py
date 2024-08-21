from enum import Enum
from pydantic import BaseModel

FLOW_NAME = "data_characterization_plugin"
CHANGELOG_FILE = "liquibase-characterization.xml"

class DCOptionsType(BaseModel):
    schemaName: str
    databaseCode: str
    cdmVersionNumber: str
    vocabSchemaName: str
    releaseDate: str
    resultsSchema: str
    excludeAnalysisIds: str
    
    @property
    def flowName(self) -> str:
        return FLOW_NAME

    @property
    def changelogFile(self) -> str:
        return CHANGELOG_FILE


class LiquibaseAction(str, Enum):
    UPDATE = "update"  # Create and update schema
    

class DatabaseDialects(str, Enum):
    HANA = "hana"
    POSTGRES = "postgresql"