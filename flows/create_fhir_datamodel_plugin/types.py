from pydantic import BaseModel
class CreateFhirDataModelOptions(BaseModel):
    database_code: str
    schema_name: str
    vocab_schema: str

    @property
    def use_cache_db(self) -> str:
        return True