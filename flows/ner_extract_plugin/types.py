from pydantic import BaseModel

PATH_TO_EXTERNAL_FILES = r"ner_extract_plugin/external"

class NerExtractType(BaseModel):
    # currently load prescription sample doc
    # TODO: can be any doc
    doc:str=f"{PATH_TO_EXTERNAL_FILES}/transcript1.txt" 
