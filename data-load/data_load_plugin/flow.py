import pandas as pd
from prefect import flow
# from prefect_shell import ShellOperation
from prefect.task_runners import SequentialTaskRunner
from sqlalchemy import create_engine
from utils.types import DataloadOptions

@flow(log_prints=True, task_runner=SequentialTaskRunner)
def data_load_plugin(options: DataloadOptions):
    # TODO
    # empty_string_to_null: bool

    files = options.files
    header = 0 if options.header else None
    escape_char = options.escape_character
    schema = options.schema_name
    truncate_tables = [f.name for f in files if f.truncate]
    chunksize = options.chunksize
    # TODO: make below dialect agnostic
    engine = create_engine('postgresql://postgres:Toor1234@alp-minerva-postgres-1:5432/alpdev_pg')
    
    # Truncating
    for t in truncate_tables:
        engine.execute(f"TRUNCATE TABLE %s", t)
    
    try:
        for file in files:
            # Load data from CSV file
            data = pd.read_csv(file.path, escapechar=escape_char, header=header, delimiter=options.delimiter, encoding=options.encoding)
            table_name = file.name

            if(header):
                csv_column_names = data.columns.tolist()
                table_column_names = [col[0] for col in engine.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '%s'", table_name).fetchall()]

                common_columns = list(set(csv_column_names) & set(table_column_names))
                data[common_columns].to_sql(table_name, engine, if_exists='append', index=False, schema=schema, chunksize=chunksize)

            else:
                data.to_sql(table_name, engine, if_exists="append", index=False, schema=schema)
    except Exception as e:
        raise e

if __name__ == '__main__':
    options = {
        "files": [
            {"name": "care_site", "path": "/tmp/data/care_site.csv", "truncate": True}
        ],
        "schema_name": "cdmvocab",
        "header": True,
        "delimiter": ","
        # TODO: test chunksize, delimiter, encoding, escape_character, truncate, any error cases
    }
    data_load_plugin(options)