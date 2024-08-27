import os
import duckdb
from prefect import get_run_logger
from create_cachedb_file_plugin.config import CreateDuckdbDatabaseFileType, CreateDuckdbDatabaseFileModules

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
    duckdb_database_name = "cdw_config_svc_validation" if create_for_cdw_config_validation else f"{database_code}_{schema_name}"
    
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
    _remove_existing_file_if_exists(
        duckdb_database_name, create_for_cdw_config_validation)

    # TODO: Add switch case after unifiying envConverter postgres dialect value
    copy_postgres_to_duckdb(dbdao, duckdb_database_name, create_for_cdw_config_validation)
    logger.info(f"""Duckdb database file: {
                duckdb_database_name} has been successfully created.""")


def copy_postgres_to_duckdb(dbdao, duckdb_database_name: str, create_for_cdw_config_validation: bool):
    logger = get_run_logger()
    
    # Get table names from db
    # Include views when creating duckdb file for cdw config validation
    table_names = dbdao.get_table_names(include_views=create_for_cdw_config_validation)

    # Get credentials for database code
    tenant_configs = dbdao.get_tenant_configs(dbdao.schema_name)

    # copy tables from postgres into duckdb
    for table in table_names:
        try:
            logger.info(f"Copying table: {table} from postgres into duckdb...")
            
            duckdb_file_path = _resolve_duckdb_file_path(
                duckdb_database_name, create_for_cdw_config_validation)
            
            with duckdb.connect(f"{os.getenv('DUCKDB__DATA_FOLDER')}/{duckdb_database_name}") as con:
                
                # If create_for_cdw_config_validation is True, add a LIMIT 0 to select statement so that only an empty table is created
                limit_statement = "LIMIT 0" if create_for_cdw_config_validation else ""
                
                result = con.execute(
                    f"""CREATE TABLE {duckdb_database_name}."{table}" AS FROM (SELECT * FROM postgres_scan('host={tenant_configs['host']} port={tenant_configs['port']} dbname={
                        tenant_configs['databaseName']} user={tenant_configs['adminUser']} password={tenant_configs['adminPassword']}', '{dbdao.schema_name}', '{table}'){limit_statement})"""
                ).fetchone()
                logger.info(f"{result[0]} rows copied")
        except Exception as err:
            logger.error(f"Table:{table} loading failed with error: {err}f")
            raise (err)
    logger.info("Postgres tables succesfully copied into duckdb database file")

def _remove_existing_file_if_exists(duckdb_database_name: str, create_for_cdw_config_validation: bool):
    logger = get_run_logger()
    duckdb_file_path = _resolve_duckdb_file_path(
        duckdb_database_name, create_for_cdw_config_validation)
    if os.path.isfile(duckdb_file_path):
        logger.info(f"Removing existing duckdb file at {duckdb_file_path}")
        os.remove(duckdb_file_path)


def _resolve_duckdb_file_path(duckdb_database_name: str, create_for_cdw_config_validation: bool):
    '''
    Gets duckdb data folder based on create_for_cdw_config_validation flag
    '''
    if create_for_cdw_config_validation:
        # Returns a hardcoded file path when creating duckdb file for cdw_config validation
        return f"{os.getenv('CDW_CONFIG_DUCKDB__DATA_FOLDER')}/{duckdb_database_name}"
    else:
        return f"{os.getenv('DUCKDB__DATA_FOLDER')}/{duckdb_database_name}"

if __name__ == '__main__':
    database_code = "alpdev_pg"
    schema_name = "cdmdefault"
    options = CreateDuckdbDatabaseFileType(
        databaseCode=database_code,
        schemaName=schema_name,
    )
    create_duckdb_database_file(options)
