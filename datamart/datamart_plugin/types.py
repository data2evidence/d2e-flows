from enum import Enum
from pydantic import BaseModel
from typing import Optional, List


class DatabaseDialects(str, Enum):
    HANA = "hana"
    POSTGRES = "postgresql"


class DatamartCopyTableConfig(BaseModel):
    tableName: str
    columnsToBeCopied: List[str]
 

class DatamartCopyConfig(BaseModel):
    timestamp: Optional[str]
    tableConfig: Optional[List[DatamartCopyTableConfig]]
    patientsToBeCopied: Optional[List[int]]
    

class DatamartOptionsType(BaseModel):
    sourceSchema: str
    vocabSchema: str
    snapshotCopyConfig: DatamartCopyConfig


class DatamartFlowAction(str, Enum):
    CREATE_SNAPSHOT = "create_snapshot"  # Copy as a new db schema
    CREATE_PARQUET_SNAPSHOT = "create_parquet_snapshot"  # Copy as parquet file
    GET_VERSION_INFO = "get_version_info" 


class CreateDatamartOptions(BaseModel):
    flow_action_type: DatamartFlowAction
    dialect: Optional[str]
    schema_name: Optional[str]
    source_schema: Optional[str]
    database_code: Optional[str]
    snapshot_copy_config: Optional[DatamartCopyConfig]
    datasets: Optional[List] = None
    token: Optional[str] = ""


RELEASE_VERSION_MAPPING = {
            "5.3": "v5.3.2",
            "5.4": "v5.4.1"
        }

class CDMVersion(str, Enum):
    OMOP53 = "5.3"
    OMOP54 = "5.4"


class EntityCountDistributionType(BaseModel):
    OBSERVATION_PERIOD_COUNT: str
    DEATH_COUNT: str
    VISIT_OCCURRENCE_COUNT: str
    VISIT_DETAIL_COUNT: str
    CONDITION_OCCURRENCE_COUNT: str
    DRUG_EXPOSURE_COUNT: str
    PROCEDURE_OCCURRENCE_COUNT: str
    DEVICE_EXPOSURE_COUNT: str
    MEASUREMENT_COUNT: str
    OBSERVATION_COUNT: str
    NOTE_COUNT: str
    EPISODE_COUNT: str
    SPECIMEN_COUNT: str
    
