from pydantic import BaseModel

class OmopCDMPluginOptions(BaseModel):
    database_code: str
    schema_name: str
    cdm_version: str