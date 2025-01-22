import os, glob
import pandas as pd
from io import StringIO

from prefect import flow, task
from prefect.logging import get_run_logger

from flows.mimic_load_plugin.types import *
from shared_utils.dao.DBDao import DBDao
from sqlalchemy import text

def raw_sql_script(conn, sql_ls):
    # cannot handle copy
    for sql_file in sql_ls:
        with open(sql_file, 'r') as file:
            try:
                sql_qry = file.read()
                conn.execute(text(sql_qry))
                conn.commit()
            except:
                conn.rollback()

@task(log_prints=True)
def create_table(conn):
    sql_ls = [PostgresCreateSql]
    raw_sql_script(conn, sql_ls)

@task(log_prints=True)
def load_data(conn, mimic_dir):
    logger = get_run_logger()
    csv_files = glob.glob(os.path.join(mimic_dir, "**/*.csv*"), recursive=True)
    for file_path in csv_files:
        table_name, dir_name = make_table_name(file_path)
        if not dir_name in ['hosp','icu']:
            continue
        try:
            load_single_table(conn, table_name, file_path)
            logger.info(f"Loading {table_name} done")
        except Exception as e:
            logger.info(f"Error loading {file_path}: {str(e)}")
    # Add constraint to tables
    sql_ls = [PostgresConstraintSql, PostgresIndexSql]
    raw_sql_script(conn, sql_ls)

def load_single_table(conn, table_name, file_path):    
    raw_conn = conn.connection.driver_connection
    with raw_conn.cursor() as cur:
        for chunk_buffer in chunk_generator(file_path):
            sql_qry = f"COPY {table_name} FROM STDIN WITH CSV"
            cur.copy_expert(sql_qry, chunk_buffer)
        raw_conn.commit()

def chunk_generator(file_path, chunksize=100000):
    for chunk in pd.read_csv(file_path, delimiter=',', encoding='utf-8', chunksize=chunksize, keep_default_na=False):
        buffer = StringIO()
        chunk.to_csv(buffer, index=False, header=False)  # No header for chunks after first
        buffer.seek(0)
        yield buffer

def make_table_name(file_path):
    # strip leading directories (e.g., ./icu/hello.csv.gz -> hello.csv.gz)
    base_name = os.path.basename(file_path)
    # strip suffix (e.g., hello.csv.gz -> hello; hello.csv -> hello)
    table_name = os.path.splitext(base_name)[0]
    # strip basename (e.g., ./icu/hello.csv.gz -> ./icu)
    path_name = os.path.dirname(file_path)
    # strip leading directories from PATHNAME (e.g. ./icu -> icu)
    dir_name = os.path.basename(path_name)
    return f"mimiciv_{dir_name}.{table_name}", dir_name

@flow(log_prints=True, persist_result=True)
def mimic_load_plugin(options: MimicLoadOptionsType):
    logger = get_run_logger()
    use_cache_db = options.use_cache_db
    database_code = options.database_code
    mimic_dir = options.mimic_dir
    dbdao = DBDao(use_cache_db=use_cache_db,
            database_code=database_code,
            connect_to_duckdb=use_cache_db,
            )
    with dbdao.engine.connect() as conn:
        conn.connection.set_client_encoding('UTF8')
        logger.info("<-------- Creating MIMIC Tables -------->")
        create_table(conn)
        logger.info("<-------- Loading MIMIC Tables -------->")
        load_data(conn, mimic_dir)
