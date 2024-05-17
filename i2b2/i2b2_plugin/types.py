from pydantic import BaseModel

class i2b2PluginType(BaseModel):
    database_code: str
    schema_name: str
    tag_name: str