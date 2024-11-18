import os
from prefect import get_run_logger

DUCKDB_EXTENSION_FILEPATH = "/app/duckdb_extensions"

def resolve_duckdb_file_path(duckdb_database_name: str, create_for_cdw_config_validation: bool):
    '''
    Gets duckdb data folder based on create_for_cdw_config_validation flag
    '''
    if create_for_cdw_config_validation:
        # Returns a hardcoded file path when creating duckdb file for cdw_config validation
        return f"{os.getenv('CDW_CONFIG_DUCKDB__DATA_FOLDER')}/{duckdb_database_name}"
    else:
        return f"{os.getenv('DUCKDB__DATA_FOLDER')}/{duckdb_database_name}"


def remove_existing_file_if_exists(duckdb_database_name: str, create_for_cdw_config_validation: bool):
    logger = get_run_logger()
    duckdb_file_path = resolve_duckdb_file_path(
        duckdb_database_name, create_for_cdw_config_validation)
    if os.path.isfile(duckdb_file_path):
        logger.info(f"Removing existing duckdb file at {duckdb_file_path}")
        os.remove(duckdb_file_path)
