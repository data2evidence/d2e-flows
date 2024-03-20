from pydantic import BaseModel, Field
from enum import Enum

FLOW_NAME = "datamodel_plugin"
DATAMODEL_CHANGELOG_MAPPING = {
            "omop5-4": "liquibase-changelog-5-4.xml"
        }

class flowActionType(str, Enum):
    CREATE_DATA_MODEL = "create"
    UPDATE_DATA_MODEL = "update"


class dataModelType(BaseModel):
    flow_action_type: flowActionType
    database_code: str = Field(...)
    data_model: str = Field(...)
    schema_name: str = Field(...)
    cleansed_schema_option: bool = Field(False)
    vocab_schema: str = Field(...)

    @property
    def flow_name(self) -> str:
        return FLOW_NAME

    @property
    def changelog_filepath(self) -> str:
        return DATAMODEL_CHANGELOG_MAPPING.get(self.data_model, None)