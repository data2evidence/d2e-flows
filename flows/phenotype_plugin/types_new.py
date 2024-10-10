from pydantic import BaseModel

class PhenotypeOptionsType(BaseModel):
    # databaseCode: str   # alpdev_pg
    # cdmschemaName: str   # cdm_5pct_9a0f90a32250497d9483c981ef1e1e70
    # cohortschemaName: str   # cdm_5pct_zhimin
    # cohorttableName: str   # cohorts_devtest1_phenotype
    # cohortsId: list   # as.integer(c(25,3,4)) or 'default'
    description: str   
    owner: str   
    # token: str   # bear token
    
    @property
    def use_cache_db(self) -> str:
        return False