from pydantic import BaseModel

class OHDSIEtlGermanyOptionsType(BaseModel):
    batchChunksize: str
    fhirGatewayJdbcCurl: str
    fhirGatewayUsername: str
    fhirGatewayPassword:str
    fhirGatewayTable: str
    omopCDMJdbcCurl: str
    omopCDMUsername: str
    omopCDMPassword: str
    omopCDMSchema: str
    dataBeginDate: str
    dataEndDate: str
    
    @property
    def use_cache_db(self) -> str:
        return False