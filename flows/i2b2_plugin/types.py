from enum import Enum
from pydantic import BaseModel
from typing import Optional, List

RELEASE_TAG_MAPPING = {
            "v1.8.1": "v1.8.1.0001"
        }

class FlowActionType(str, Enum):
    CREATE_DATA_MODEL = "create_datamodel"
    GET_VERSION_INFO = "get_version_info"

class i2b2PluginType(BaseModel):
    flow_action_type: FlowActionType
    database_code: str = ""
    schema_name: Optional[str]
    data_model: str
    load_demo_data: bool = False
    datasets: Optional[List] = None
    
    @property
    def tag_name(self) -> str:
        return RELEASE_TAG_MAPPING.get(self.data_model)
    
    @property
    def use_cache_db(self) -> str:
        return False