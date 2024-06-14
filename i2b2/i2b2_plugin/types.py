from pydantic import BaseModel
from typing import Optional

class i2b2PluginType(BaseModel):
    database_code: str
    schema_name: str
    tag_name: str
    load_data: Optional[bool]
    
