from pydantic import BaseModel


class cohortSurvivalOptionsType(BaseModel):
    databaseCode: str
    schemaName: str
    targetCohortDefinitionId: int
    outcomeCohortDefinitionId: int
    datasetId: str
    # competing_outcome_cohort_definition_id: int
    # strata: unsure
