from pydantic import BaseModel
from typing import Optional
from enum import Enum

Concept_Standard = 'flows/loyalty_score_plugin/external/concept_ls_Standard.csv'
Coefficeints = 'flows/loyalty_score_plugin/external/coefficients.json'

class FlowActionType(str, Enum):
    LOYALTY_SCORE = "calculate"
    RETRAIN_ALGO = "retrain"

class LoyaltyPluginType(BaseModel):
    mode: str
    schema_name: str
    database_code: str
    index_date: str
    lookback_years: int
    return_years: int = 0
    test_ratio: float = 0
    coeff_table_name: Optional[str] 
    loyalty_cohort_table_name: Optional[str] # table name to store the loyalty score result
    retraincoeff_table_name: Optional[str]

    @property
    def use_cache_db(self) -> str:
        return False
