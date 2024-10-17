from prefect.logging import get_run_logger
from prefect import flow
from flows.create_fhir_datamodel_plugin.types import *
from flows.create_fhir_datamodel_plugin.fhirUtils import readJsonFileAndCreateDuckdbTables

@flow(log_prints=True)
def create_fhir_datamodel_plugin(options: CreateFhirDataModelOptions):
    logger = get_run_logger()
    
    database_code = options.database_code
    schema_name = options.schema_name
    
    try:
        readJsonFileAndCreateDuckdbTables(database_code=database_code, schema_name=schema_name)
    except Exception as e:
        logger.error(e)