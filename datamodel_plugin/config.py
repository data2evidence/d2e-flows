from pydantic import BaseModel, Field
from enum import Enum

PACKAGE_NAME = "datamodel_plugin"
DATAMODEL_CHANGELOG_MAPPING = {
            "omop5-4": "liquibase-changelog-5-4.xml"
        }

class flowActionType(str, Enum):
    CREATE_DATA_MODEL = "create"
    UPDATE_DATA_MODEL = "update"


class dataModelType(BaseModel):
    flow_action_type: flowActionType
    db_name: str = Field(...)
    data_model: str = Field(...)
    schema_name: str = Field(...)
    cleansed_schema_option: bool = Field(False)

    @property
    def package_name(self) -> str:
        return PACKAGE_NAME

    @property
    def changelog_filepath(self) -> str:
        return DATAMODEL_CHANGELOG_MAPPING.get(self.data_model, None)
    
class createDataModelType(BaseModel):
    db_name: str = Field(...)
    data_model: str = Field(...)
    schema_name: str = Field(...)
    cleansed_schema_option: bool = Field(False)
    
    @property
    def package_name(self) -> str:
        return PACKAGE_NAME
    
    @property
    def changelog_filepath(self) -> str:
        return DATAMODEL_CHANGELOG_MAPPING.get(self.data_model, None)
    
class updateDataModelType(BaseModel):
    db_name: str = Field(...)
    data_model: str = Field(...)
    schema_name: str = Field(...)

    @property
    def package_name(self) -> str:
        return PACKAGE_NAME
    
    @property
    def changelog_filepath(self) -> str:
        return DATAMODEL_CHANGELOG_MAPPING.get(self.data_model, None)