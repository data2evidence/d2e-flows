from pydantic import BaseModel
from typing import Optional
from enum import Enum


PATH_TO_EXTERNAL_FILES = r"dicom_etl_plugin/external"

class FlowActionType(str, Enum):
    INGEST_METADATA = "insert_metadata_data"


class ETLOptions(BaseModel):
    flow_action_type: FlowActionType
    database_code: str
    medical_imaging_schema_name: str
    cdm_schema_name: str
    vocab_schema_name: str
    root_folder: str
    upload_files: Optional[bool] = False
