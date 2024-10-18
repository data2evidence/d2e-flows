from pydantic import BaseModel

PATH_TO_EXTERNAL_FILES = r"external"

class NerExtractOptions(BaseModel):
    # currently load prescription sample doc
    # TODO: can be any doc
    doc:str=f"{PATH_TO_EXTERNAL_FILES}/transcript1.txt" 

    @property
    def use_cache_db(self) -> str:
        return False
