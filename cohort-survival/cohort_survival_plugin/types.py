from pydantic import BaseModel


class cohortSurvivalOptionsType(BaseModel):
    databaseCode: str
    schemaName: str
    targetCohortDefinitionId: int
    outcomeCohortDefinitionId: int
    # competing_outcome_cohort_definition_id: int
    # strata: unsure
