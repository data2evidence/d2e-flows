from prefect.logging import get_run_logger
from prefect import flow
from flows.create_fhir_datamodel_plugin.types import *
from flows.create_fhir_datamodel_plugin.fhir_utils import read_json_file_and_create_duckdb_tables

@flow(log_prints=True)
def create_fhir_datamodel_plugin(options: CreateFhirDataModelOptions):
    logger = get_run_logger()
    
    database_code = options.database_code
    schema_name = options.schema_name
    vocab_schema = options.vocab_schema
    
    try:
        read_json_file_and_create_duckdb_tables(database_code=database_code, schema_name=schema_name, vocab_schema=vocab_schema)
    except Exception as e:
        logger.error("Failed to create fhir data model", e)
        raise e