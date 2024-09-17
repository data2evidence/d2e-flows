from enum import Enum
from pydantic import BaseModel

FLOW_NAME = "data_characterization_plugin"
CHARACTERIZATION_DATA_MODEL = "characterization"
CHANGELOG_FILE = "liquibase-characterization.xml"
ACHILLES_THREAD_COUNT = 5

class DCOptionsType(BaseModel):
    schemaName: str
    databaseCode: str
    cdmVersionNumber: str
    vocabSchemaName: str
    releaseDate: str
    resultsSchema: str
    excludeAnalysisIds: str

    @property
    def use_cache_db(self) -> str:
        return False
    
    @property
    def flowName(self) -> str:
        return FLOW_NAME

    @property
    def changelogFile(self) -> str:
        return CHANGELOG_FILE
