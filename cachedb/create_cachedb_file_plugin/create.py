import duckdb
from prefect import get_run_logger
from create_cachedb_file_plugin.config import CreateDuckdbDatabaseFileType, CreateDuckdbDatabaseFileModules
from create_cachedb_file_plugin.duckdb_fts import create_duckdb_fts_index
from create_cachedb_file_plugin.duckdb_postgres import copy_postgres_to_duckdb
from create_cachedb_file_plugin.util import remove_existing_file_if_exists

# These imports are now coming in from dynamic imports as CreateDuckdbDatabaseFileModules
# from utils.types import DatabaseDialects, PG_TENANT_USERS, DatabaseDialects
# from dao.DBDao import DBDao


def get_supported_duckdb_dialetcs(modules: CreateDuckdbDatabaseFileModules):
    SUPPORTED_DUCKDB_DIALECTS = [
        modules.utils_types.DatabaseDialects.POSTGRES.value
    ]

    return SUPPORTED_DUCKDB_DIALECTS


def create_duckdb_database_file(options: CreateDuckdbDatabaseFileType, modules: CreateDuckdbDatabaseFileModules):
    logger = get_run_logger()
    SUPPORTED_DUCKDB_DIALECTS = get_supported_duckdb_dialetcs(modules)

    database_code = options.databaseCode
    schema_name = options.schemaName
    use_cache_db = options.use_cache_db
    create_for_cdw_config_validation = options.createForCdwConfigValidation

    # Set hardcoded name for duckdb databae file if create_for_cdw_config_validation is TRUE
    duckdb_database_name = "cdw_config_svc_validation_schema" if create_for_cdw_config_validation else f"{database_code}_{schema_name}"
    
    dbdao = modules.dao_DBDao.DBDao(use_cache_db=use_cache_db,
                                    database_code=database_code,
                                    schema_name=schema_name)

    # Get dialect from database code
    dialect = dbdao.db_dialect

    if dialect not in SUPPORTED_DUCKDB_DIALECTS:
        error_message = f"""Input dialect: {
            dialect} is not supported, supported dialects are: {[SUPPORTED_DUCKDB_DIALECTS]}"""
        logger.error(error_message)
        raise ValueError(error_message)

    # If file already exists, delete first before moving to copy step
    remove_existing_file_if_exists(
        duckdb_database_name)


    # TODO: Add switch case after unifiying envConverter postgres dialect value
    copy_postgres_to_duckdb(dbdao, duckdb_database_name, create_for_cdw_config_validation)

    # Dont create fulltext search index for cdw config validation duckdb files
    if not create_for_cdw_config_validation:
        create_duckdb_fts_index(dbdao, duckdb_database_name)


if __name__ == '__main__':
    database_code = "alpdev_pg"
    schema_name = "cdmdefault"
    options = CreateDuckdbDatabaseFileType(
        databaseCode=database_code,
        schemaName=schema_name,
        createForCdwConfigValidation=False
    )
    create_duckdb_database_file(options)
