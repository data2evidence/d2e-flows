from enum import Enum
from pydantic import BaseModel
from typing import Optional, List

class FlowActionType(str, Enum):
    CREATE_DATA_MODEL = "create_datamodel"
    GET_VERSION_INFO = "get_version_info"

class i2b2PluginType(BaseModel):
    flow_action_type: FlowActionType
    database_code: str = ""
    schema_name: Optional[str]
    tag_name: Optional[str]
    load_data: Optional[bool]
    datasets: Optional[List] = None
    token: Optional[str] = ""
    
    @property
    def data_model(self) -> str:
        return "i2b2"