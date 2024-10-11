from prefect import flow, get_run_logger

from flows.create_fhir_datamodel_plugin.types import *
from utils import fhirUtils

@flow(log_prints=True)
def create_fhir_datamodel_plugin(options: CreateFhirDataModelOptions):
    logger = get_run_logger()
    
    database_code = options.database_code
    schema_name = options.schema_name
    
    try:
        utils = fhirUtils(database_code, schema_name)
        utils.readJsonFileAndCreateDuckdbTables()
    except Exception as e:
        logger.error(e)