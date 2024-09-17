import duckdb
from prefect import get_run_logger
from create_cachedb_file_plugin.util import resolve_duckdb_file_path


def copy_postgres_to_duckdb(db_dao: any, duckdb_database_name: str, create_for_cdw_config_validation: bool):
    logger = get_run_logger()

    # Include views when creating duckdb file for cdw config validation
    table_names = db_dao.get_table_names(
        include_views=create_for_cdw_config_validation)

    # Get credentials for database code
    db_credentials = db_dao.tenant_configs

    # copy tables from postgres into duckdb
    for table in table_names:
        try:
            logger.info(f"Copying table: {table} from postgres into duckdb...")

            duckdb_file_path = resolve_duckdb_file_path(
                duckdb_database_name, create_for_cdw_config_validation)
            with duckdb.connect(duckdb_file_path) as con:

                # If create_for_cdw_config_validation is True, add a LIMIT 0 to select statement so that only an empty table is created
                limit_statement = "LIMIT 0" if create_for_cdw_config_validation else ""

                result = con.execute(
                    f"""CREATE TABLE {duckdb_database_name}."{table}" AS FROM (SELECT * FROM postgres_scan('host={db_credentials['host']} port={db_credentials['port']} dbname={
                        db_credentials['databaseName']} user={db_credentials['user']} password={db_credentials['password']}', '{db_dao.schema_name}', '{table}') {limit_statement})"""
                ).fetchone()
                logger.info(f"{result[0]} rows copied")
        except Exception as err:
            logger.error(f"Table:{table} loading failed with error: {err}f")
            raise (err)
    logger.info("Postgres tables succesfully copied into duckdb database file")
    logger.info(
        f"""Duckdb database file: {duckdb_database_name} successfully created.""")
