from pydantic import BaseModel

class CohortJsonType(BaseModel):
    id: int
    name: str
    created_date: int
    modified_date: int
    has_write_access: bool
    tags: list
    expression_type: str
    expression: dict

class CohortGeneratorOptionsType(BaseModel):
    database_code: str
    schema_name: str
    vocab_schema_name: str
    cohort_json: CohortJsonType
    dataset_id: str
    description: str
    owner: str
    token: str