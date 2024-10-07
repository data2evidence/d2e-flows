from pydantic import BaseModel

class PhenotypeOptionsType(BaseModel):
    databaseCode: str
    cdmschemaName: str
    cohortschemaName: str
    cohortsId: list
    description: str
    owner: str
    token: str
    
    @property
    def use_cache_db(self) -> str:
        return False