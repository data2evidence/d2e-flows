from pydantic import BaseModel


class cohortSurvivalOptionsType(BaseModel):
    database_code: str
    schema_name: str
    target_cohort_definition_id: str
    outcome_cohort_definition_id: str
