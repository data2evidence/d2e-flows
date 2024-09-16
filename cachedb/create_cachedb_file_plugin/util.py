import os
from prefect import get_run_logger


def resolve_duckdb_file_path(duckdb_database_name: str, ):
    '''
    Gets full file path of duckdb databsae file
    '''    
    return f"{os.getenv('DUCKDB__DATA_FOLDER')}/{duckdb_database_name}"


def remove_existing_file_if_exists(duckdb_database_name: str):
    logger = get_run_logger()
    duckdb_file_path = resolve_duckdb_file_path(
        duckdb_database_name)
    if os.path.isfile(duckdb_file_path):
        logger.info(f"Removing existing duckdb file at {duckdb_file_path}")
        os.remove(duckdb_file_path)
