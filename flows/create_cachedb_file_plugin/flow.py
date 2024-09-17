from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from flows.create_cachedb_file_plugin.utils import *
from flows.create_cachedb_file_plugin.duckdb_fts import create_duckdb_fts_index
from flows.create_cachedb_file_plugin.config import CreateDuckdbDatabaseFileType
from flows.create_cachedb_file_plugin.duckdb_postgres import copy_postgres_to_duckdb

from shared_utils.dao.DBDao import DBDao


@flow(log_prints=True, task_runner=SequentialTaskRunner)
def create_cachedb_file_plugin(options: CreateDuckdbDatabaseFileType):

    logger = get_run_logger()
    
    database_code = options.databaseCode
    schema_name = options.schemaName
    use_cache_db = options.use_cache_db
    create_for_cdw_config_validation = options.createForCdwConfigValidation


    # Set hardcoded name for duckdb databae file if create_for_cdw_config_validation is TRUE
    duckdb_database_name = "cdw_config_svc_validation" if create_for_cdw_config_validation else f"{database_code}_{schema_name}"
    
    dbdao = DBDao(use_cache_db=use_cache_db,
                  database_code=database_code,
                  schema_name=schema_name)

    remove_existing_file_if_exists(duckdb_database_name, create_for_cdw_config_validation, logger)


    # TODO: Add switch case after unifiying envConverter postgres dialect value
    copy_postgres_to_duckdb(dbdao, duckdb_database_name, create_for_cdw_config_validation)

    # Dont create fulltext search index for cdw config validation duckdb files
    if not create_for_cdw_config_validation:
        create_duckdb_fts_index(dbdao, duckdb_database_name,
                                create_for_cdw_config_validation)



if __name__ == '__main__':
    database_code = "alpdev_pg"
    schema_name = "cdmdefault"
    options = CreateDuckdbDatabaseFileType(
        databaseCode=database_code,
        schemaName=schema_name,
        createForCdwConfigValidation=False
    )
    create_cachedb_file_plugin(options)
