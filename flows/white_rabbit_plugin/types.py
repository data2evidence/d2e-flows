from pydantic import BaseModel, Field

class ServiceCredentials(BaseModel):
    PG__DB_NAME: str = Field(..., strict = True)
    PG__PORT: str = Field(..., strict = True)
    PG__HOST: str = Field(..., strict = True)
    PERSEUS__FILES_MANAGER_HOST: str = Field(..., strict = True)
    PG_ADMIN_USER: str = Field(..., strict = True)
    PG_ADMIN_PASSWORD: str = Field(..., strict = True)