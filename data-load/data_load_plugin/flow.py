import sys
print(sys.path)

import importlib
import pandas as pd
from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from sqlalchemy import create_engine, text
from data_load_plugin.utils.types import DataloadOptions

@flow(log_prints=True, task_runner=SequentialTaskRunner)
def data_load_plugin(options: DataloadOptions):

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
            # Load data from CSV file
            for i, data in read_csv(file.path, escapechar=escape_char, header=header, delimiter=options.delimiter, encoding=options.encoding, chunksize=chunksize):
                table_name = file.table_name

                if(header):
                    csv_column_names = data.columns.tolist()
                    table_column_names = [col[0] for col in engine.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", table_name).fetchall()]

                    common_columns = list(set(csv_column_names) & set(table_column_names))
                    data[common_columns].to_sql(table_name, engine, if_exists='append', index=False, schema=schema, chunksize=chunksize)

                else:
                    data.to_sql(table_name, engine, if_exists="append", index=False, schema=schema, chunksize=chunksize)
        except Exception as e:
            logger.error(f'Data load failed for the table {table_name} at the chunk index: {i}  with error: {e}')

def read_csv(filepath, escapechar, header, delimiter, encoding, chunksize):
    i = 1
    if chunksize:
        for chunk in pd.read_csv(filepath, escapechar=escapechar, header=header, delimiter=delimiter, encoding=encoding, chunksize=chunksize):
            yield i, chunk
            i += 1
    else:
        yield i, pd.read_csv(filepath)

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
    data_load_plugin(DataloadOptions(**options))