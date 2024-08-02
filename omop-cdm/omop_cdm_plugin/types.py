from enum import Enum
from pydantic import BaseModel
from typing import Optional, List

# Todo: call github api to get latest release
RELEASE_VERSION_MAPPING = {
            "5.3": "v5.3.2",
            "5.4": "v5.4.1"
        }

class FlowActionType(str, Enum):
    CREATE_DATA_MODEL = "create_datamodel"
    GET_VERSION_INFO = "get_version_info"


class CDMVersion(str, Enum):
    OMOP53 = "5.3"
    OMOP54 = "5.4"

class OmopCDMPluginOptions(BaseModel):
    flow_action_type: FlowActionType
    database_code: str
    data_model: str # omop5-3, omop5-4
    schema_name: Optional[str]
    vocab_schema: Optional[str]
    datasets: Optional[List] = None
    token: Optional[str] = ""

    @property
    def cdm_version(self) -> str:
        return self.data_model[-3:].replace("-", ".")
    
    @property
    def release_version(self) -> str:
        return RELEASE_VERSION_MAPPING.get(self.cdm_version)
    

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