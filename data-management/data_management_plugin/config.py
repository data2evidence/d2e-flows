from enum import Enum
from pydantic import BaseModel, Field, root_validator
from typing import Optional, Dict, List

FLOW_NAME = "data_management_plugin"
DATAMODEL_CHANGELOG_MAPPING = {
            "omop5-4": "liquibase-changelog-5-4.xml",
            "medical-imaging": "liquibase-changelog-medical-imaging.xml"
        }

class flowActionType(str, Enum):
    CREATE_DATA_MODEL = "create_datamodel"
    UPDATE_DATA_MODEL = "update_datamodel"
    ROLLBACK_COUNT = "rollback_count"
    ROLLBACK_TAG = "rollback_tag"
    GET_VERSION_INFO = "get_version_info"
    CREATE_CDMSCHEMA = "create_cdm_schema"
    CHANGELOG_SYNC = "changelog_sync"

class dataModelType(BaseModel):
    flow_action_type: flowActionType
    database_code: str
    data_model: str
    schema_name: Optional[str]
    cleansed_schema_option: Optional[bool]
    vocab_schema: Optional[str]
    rollback_count: Optional[int]
    rollback_tag: Optional[str]
    update_count: Optional[int]
    token: Optional[str]
    datasets: Optional[List]

    @property
    def flow_name(self) -> str:
        return FLOW_NAME

    @property
    def changelog_filepath(self) -> str:
        return DATAMODEL_CHANGELOG_MAPPING.get(self.data_model, None)
    
    @property
    def changelog_filepath_list(self) -> Dict:
        return DATAMODEL_CHANGELOG_MAPPING
    
    @root_validator(pre=True)
    def set_default_vocab_schema(cls, values):
        if values.get('vocab_schema') is None:
            values['vocab_schema'] = values.get('schema_name')
        return values
