import os
import pandas as pd

from prefect.logging.loggers import task_run_logger



def drop_schema_hook(task, task_run, state, schema_dao):
    logger = task_run_logger(task_run, task)
    logger.info(
        f"Dropping schema {schema_dao.database_code}.{schema_dao.schema_name}")
    try:
        drop_schema = schema_dao.drop_schema()
        msg = f"Successfully drop schema {schema_dao.database_code}.{schema_dao.schema_name}"
        logger.info(msg)
        logger.info("Do nothing")
    except Exception as e:
        logger.error(
            f"Failed to drop schema {schema_dao.database_code}.{schema_dao.schema_name}")
        raise e



def get_export_to_ares_execute_error_message_from_file(outputFolder: str, schema_name):
    ares_path = os.path.join(outputFolder, schema_name[:25] if len(schema_name) > 25 else schema_name)
    # Get name of folder created by at {outputFolder/schema_name}

    cdm_release_date = os.listdir(ares_path)[0]
    with open(os.path.join(ares_path, cdm_release_date, "errorReportSql.txt"), 'rt') as f:
        error_message = f.read()
    return error_message


def get_export_to_ares_results_from_file(outputFolder: str, schema_name):
    ares_path = os.path.join(outputFolder, schema_name[:25] if len(schema_name) > 25 else schema_name)
    # Get name of folder created by at {outputFolder/schema_name}

    cdm_release_date = os.listdir(ares_path)[0]

    # export_to_ares creates many csv files, but now we are only interested in saving results from records-by-domain.csv
    # Read records-by-domain.csv and parse csv into json
    file_name = "records-by-domain"
    df = pd.read_csv(os.path.join(
        ares_path, cdm_release_date, f"{file_name}.csv"))
    df = df.rename(columns={"count_records": "countRecords"})

    data = {
        "exportToAres": {
            "cdmReleaseDate": cdm_release_date,
            file_name: df.to_dict(orient="records")
        }
    }

    return data
