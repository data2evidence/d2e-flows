from pydantic import BaseModel

FLOW_NAME = "data_characterization_plugin"
CHANGELOG_FILE = "liquibase-characterization.xml"


class dcOptionsType(BaseModel):
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