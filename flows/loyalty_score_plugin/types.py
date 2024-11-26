from pydantic import BaseModel
from typing import Optional, List, Dict
from enum import Enum

Concept_Standard = 'flows/loyalty_score_plugin/external/concept_ls_Standard.csv'
Coefficeints = 'flows/loyalty_score_plugin/external/coefficients.json'

class FlowActionType(str, Enum):
    LOYALTY_SCORE = "calculate_loyalty_score"
    RETRAIN_ALGO = "retrain_loyalty_algo"

class LoyaltyPluginType(BaseModel):
    mode: str
    # match mode:
    #     case FlowActionType.LOYALTY_SCORE:

    schemaName: str
    databaseCode: str
    indexDate: str
    lookbackYears: int
    returnYears: int = 0
    testRatio: float
    coeffTableName: Optional[str] 
    loyaltycohortTableName: Optional[str] # table name to store the loyalty score result
    retrainCoeffTableName: Optional[str]

    @property
    def use_cache_db(self) -> str:
        return False
