from typing import List, Optional
from pydantic import BaseModel

class File(BaseModel):
    path: str
    name: str
    truncate: Optional[bool] = False

class DataloadOptions(BaseModel):
    files: List[File]
    schema_name: str
    header: Optional[bool] = True
    delimiter: Optional[str] = ','
    escape_character: Optional[str] = None
    encoding: Optional[str] = None
    empty_string_to_null: Optional[bool] = None
    chunksize: Optional[int] = None