import importlib
import sys
from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from sqlalchemy import create_engine, text
from data_load_plugin.utils.types import DataloadOptions
sys.path.append('/usr/local/lib/python3.10/dist-packages/pandas')
import pandas as pd

def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    
    
@flow(log_prints=True, task_runner=SequentialTaskRunner)
def data_load_plugin(options: DataloadOptions):
    setup_plugin()
    logger = get_run_logger()
    files = options.files
    database_code = options.database_code
    header = 0 if options.header else None
    escape_char = options.escape_character if options.escape_character else None
    schema = options.schema_name
    tables_to_truncate = [f.table_name for f in files if f.truncate]
    chunksize = options.chunksize if options.chunksize else None
    dbutils_module = importlib.import_module('alpconnection.dbutils')
    conn_details = dbutils_module.extract_db_credentials(database_code)
    database_name = conn_details["databaseName"]
    pg_user = conn_details["adminUser"]
    pg_password = conn_details["adminPassword"]
    pg_host = conn_details["host"]
    pg_port = conn_details["port"]

    # TODO: make below dialect agnostic
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{database_name}')

    # Truncating
    with engine.connect() as connection:
        for t in tables_to_truncate:
            trans = connection.begin()
            try:
                truncate_sql = text(f"delete from {schema}.{t}")
                connection.execute(truncate_sql)
                trans.commit()
                logger.info(f"Table {t} truncated successfully.")
            except Exception as e:
                trans.rollback()
                logger.error(e)

    for file in files:
        try:
            logger.info(f"Reading data from file {file.table_name}")
            # Load data from CSV file
            for i, data in read_csv(file.path, escapechar=escape_char, header=header, delimiter=options.delimiter, encoding=options.encoding, chunksize=chunksize):
                if(header):
                    csv_column_names = data.columns.tolist()
                    table_column_names = [col[0] for col in engine.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", file.table_name).fetchall()]

                    common_columns = list(set(csv_column_names) & set(table_column_names))
                    logger.info(f"Inserting data into table {file.table_name} at the chunk index of {i}")
                    if (file.table_name == "concept_relationship" or file.table_name == "concept" or file.table_name == "drug_strength"):
                        logger.info(f"Cast column data in {file.table_name} dataframe")
                        data["valid_start_date"] = pd.to_datetime(data["valid_start_date"].astype(str))
                        data["valid_end_date"] =  pd.to_datetime(data["valid_end_date"].astype(str))
                    if(file.table_name == "drug_strength"):
                        data["amount_value"] = data["amount_value"].replace('', pd.NA)
                        data["amount_unit_concept_id"] = data["amount_unit_concept_id"].replace("", pd.NA)
                        data["box_size"] = data["box_size"].replace('', pd.NA)
                        data["numerator_value"] = data["numerator_value"].replace('', pd.NA)
                        data["numerator_unit_concept_id"] = data["numerator_unit_concept_id"].replace('', pd.NA)
                        data["denominator_unit_concept_id"] = data["denominator_unit_concept_id"].replace('', pd.NA)
                        data["denominator_value"] = data["denominator_value"].replace('', pd.NA)
                    data = data.replace("N/A", "N/A")
                    data[common_columns].to_sql(file.table_name, engine, if_exists='append', index=False, schema=schema, chunksize=chunksize)
                else:
                    logger.info(f"Inserting data into table {file.table_name} at the chunk index of {i}")
                    if (file.table_name == "concept_relationship" or file.table_name == "concept" or file.table_name == "drug_strength"):
                        logger.info(f"Cast column data in {file.table_name} dataframe")
                        data["valid_start_date"] = pd.to_datetime(data["valid_start_date"].astype(str))
                        data["valid_end_date"] =  pd.to_datetime(data["valid_end_date"].astype(str))
                    if(file.table_name == "drug_strength"):
                        data["amount_value"] = data["amount_value"].replace('', pd.NA)
                        data["amount_unit_concept_id"] = data["amount_unit_concept_id"].replace('', pd.NA)
                        data["box_size"] = data["box_size"].replace('', pd.NA)
                        data["numerator_value"] = data["numerator_value"].replace('', pd.NA)
                        data["numerator_unit_concept_id"] = data["numerator_unit_concept_id"].replace('', pd.NA)
                        data["denominator_unit_concept_id"] = data["denominator_unit_concept_id"].replace('', pd.NA)
                        data["denominator_value"] = data["denominator_value"].replace('', pd.NA)
                    data = data.replace("N/A", "N/A")
                    data.to_sql(file.table_name, engine, if_exists="append", index=False, schema=schema, chunksize=chunksize)
        except Exception as e:
            logger.error(f'Data load failed for the table {file.table_name} at the chunk index: {i}  with error: {e}')

def read_csv(filepath, escapechar, header, delimiter, encoding, chunksize):
    i = 1
    if chunksize:
        for chunk in pd.read_csv(filepath, escapechar=escapechar, header=header, delimiter=delimiter, encoding=encoding, chunksize=chunksize, keep_default_na=False):
            yield i, chunk
            i += 1
    else:
        yield i, pd.read_csv(filepath)