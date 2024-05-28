import importlib
import pandas as pd
from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from sqlalchemy import create_engine
from utils.types import DataloadOptions

@flow(log_prints=True, task_runner=SequentialTaskRunner)
def data_load_plugin(options: DataloadOptions):

    logger = get_run_logger()
    files = options.files
    database_code = options.database_code
    header = 0 if options.header else None
    escape_char = options.escape_character
    schema = options.schema_name
    tables_to_truncate = [f.table_name for f in files if f.truncate]
    chunksize = options.chunksize
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
    for t in tables_to_truncate:
        engine.execute(f"TRUNCATE TABLE %s", t)

    for file in files:
        try:
            # Load data from CSV file
            data = pd.read_csv(file.path, escapechar=escape_char, header=header, delimiter=options.delimiter, encoding=options.encoding)
            table_name = file.table_name

            if(header):
                csv_column_names = data.columns.tolist()
                table_column_names = [col[0] for col in engine.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", table_name).fetchall()]

                common_columns = list(set(csv_column_names) & set(table_column_names))
                data[common_columns].to_sql(table_name, engine, if_exists='append', index=False, schema=schema, chunksize=chunksize)

            else:
                data.to_sql(table_name, engine, if_exists="append", index=False, schema=schema, chunksize=chunksize)
        except Exception as e:
            logger.error(e)

if __name__ == '__main__':
    options = {
        "database_code": "",
        "files": [
            {"table_name": "care_site", "path": "/tmp/data/care_site.csv", "truncate": True}
        ],
        "schema_name": "cdmvocab",
        "header": True,
        "delimiter": ","
    }
    data_load_plugin(options)