from prefect import task
from prefect.logging import get_run_logger
from sqlalchemy import text

import numpy as np
from flows.mimic_omop_conversion_plugin.const import *
from flows.mimic_omop_conversion_plugin.utils import *
from flows.mimic_omop_conversion_plugin.load_data import *
from shared_utils.types import SupportedDatabaseDialects


@task(log_prints=True)
def staging_mimic_data(conn):
    create_schema(conn, 'mimic_etl')
    raw_sql_files(conn, StagDir, StagSql)


@task(log_prints=True)
def ETL_transformation(conn):
    logger = get_run_logger()
    try:
        raw_sql_files(conn, ETLDir, ETLSqls)
    except Exception as e:
        logger.error(f"Error tranforming mimic: {str(e)}")
        raise Exception()


@task(log_prints=True)
def final_cdm_tables(conn):
    logger = get_run_logger()
    try:
        create_schema(conn, 'cdm')
        raw_sql_files(conn, CdmDir, CdmSqls)
    except Exception as e:
        logger.error(f"Error creating final cdm data: {str(e)}")
        raise Exception()


@task(log_prints=True) 
def export_data(conn, to_dbdao, chunk_size):
    db_credentials = to_dbdao.tenant_configs
    # print('db_credentials', db_credentials)
    dialect = to_dbdao.dialect
    schema_name = to_dbdao.schema_name
    if to_dbdao.check_schema_exists(): 
        to_dbdao.drop_schema()
    to_dbdao.create_schema()
    match dialect:
        case SupportedDatabaseDialects.POSTGRES:
            attach_qry = f"""ATTACH 'host={db_credentials.host} port={db_credentials.port} dbname={db_credentials.databaseName} 
            user={db_credentials.adminUser} password={db_credentials.adminPassword.get_secret_value()}' 
            AS pg (TYPE POSTGRES, SCHEMA {schema_name});
            """
            # Attach Posgres Database
            conn.execute(attach_qry)
            # Creat schema and tables in postgres database
            creat_table(conn, PostgresDDL, schema_name=schema_name)
            tables = conn.execute(f"SELECT table_name FROM duckdb_tables() WHERE (database_name = 'pg')").fetchall()
            tables = [x[0] for x in tables]
            for table in tables:
                conn.execute(f"""
                INSERT INTO pg.{schema_name}.{table}
                SELECT * FROM cdm.{table};    
                """)
            conn.execute("DETACH pg;")

        case SupportedDatabaseDialects.HANA:
            create_sqls = open(HANADDL).read().replace('@schema_name', schema_name).split(';')[:-1]
            for create_sql in create_sqls:
                with to_dbdao.engine.connect() as hana_conn:
                    hana_conn.execute(text(create_sql))
                    hana_conn.commit()

            tables = to_dbdao.get_table_names()
            for table in tables:
                for chunk in read_table_chunks(conn, table, chunk_size=chunk_size):                   
                    if not chunk.empty:
                        insert_to_hana_direct(to_dbdao, chunk, schema_name, table)
                    else: 
                        break


def read_table_chunks(conn, table, chunk_size):
    count = conn.execute(f"SELECT COUNT(*) FROM cdm.{table}").fetchone()[0]
    for offset in range(0, count, chunk_size):
        chunk = conn.execute(f"""
            SELECT * FROM cdm.{table}
            LIMIT {chunk_size} OFFSET {offset}
        """).df()
        yield chunk

def insert_to_hana_direct(to_dbdao, chunk, schema_name, table):
    with to_dbdao.engine.connect() as hana_conn:
        # Use Upper case for HANA
        chunk.columns = chunk.columns.str.upper()
        # Replace np.nan with None
        chunk.replace([np.nan], [None], inplace=True)
        columns = chunk.columns.tolist()
        columns_str = ', '.join(f'"{col}"' for col in columns)
        placeholders = ','.join(f':{col}' for col in columns)
        insert_stmt = f'INSERT INTO {schema_name}.{table} ({columns_str}) VALUES ({placeholders})'
        data = chunk.to_dict('records')
        hana_conn.execute(text(insert_stmt), data)
        hana_conn.commit()

