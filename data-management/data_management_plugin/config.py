from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional, Dict

FLOW_NAME = "data_management_plugin"
DATAMODEL_CHANGELOG_MAPPING = {
            "omop5-4": "liquibase-changelog-5-4.xml"
        }

class flowActionType(str, Enum):
    CREATE_DATA_MODEL = "create_datamodel"
    UPDATE_DATA_MODEL = "update_datamodel"
    ROLLBACK_COUNT = "rollback_count"
    ROLLBACK_TAG = "rollback_tag"
    CREATE_SNAPSHOT = "create_snapshot"
    CREATE_PARQUET_SNAPSHOT = "create_parquet_snapshot"
    FETCH_VERSION_INFO = "fetch_version_info"
    CREATE_QUESTIONNAIRE_DEFINITION = "create_questionnaire_definition"
    GET_QUESTIONNAIRE_RESPONSE = "get_questionnaire_response"


class dataModelType(BaseModel):
    flow_action_type: flowActionType
    database_code: str = Field(...)
    data_model: str = Field(...)
    schema_name: str = Field(...)
    cleansed_schema_option: Optional[bool]
    vocab_schema: Optional[str]
    snapshot_copy_config: Optional[Dict]
    source_schema: Optional[str]
    rollback_count: Optional[int]
    rollback_tag: Optional[str]
    update_count: Optional[int]
    questionnaire_definition: Optional[Dict]
    questionnaire_id: Optional[str]

    @property
    def flow_name(self) -> str:
        return FLOW_NAME

    @property
    def changelog_filepath(self) -> str:
        return DATAMODEL_CHANGELOG_MAPPING.get(self.data_model, None)