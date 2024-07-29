import os
import sys
import importlib
import pandas as pd

from prefect.logging.loggers import task_run_logger
from prefect.server.schemas.states import StateType

#from dao.DBDao import DBDao
from data_characterization_plugin.utils.types import DCOptionsType


def drop_schema_hook(task, task_run, state, schema_dao):
    logger = task_run_logger(task_run, task)
    logger.info(
        f"Dropping schema {schema_dao.database_code}.{schema_dao.schema_name}")
    try:
        drop_schema = schema_dao.drop_schema()
        msg = f"Successfully drop schema {schema_dao.database_code}.{schema_dao.schema_name}"
        logger.info(msg)
    except Exception as e:
        logger.error(
            f"Failed to drop schema {schema_dao.database_code}.{schema_dao.schema_name}")
        raise e



def drop_data_characterization_schema(flow, flow_run, state):
    options = DCOptionsType(**flow_run.parameters['options'])
    database_code = options.databaseCode
    results_schema = options.resultsSchema
    try:
        print(f"Dropping data characterization results schema '{results_schema}'")
        sys.path.append('/app/pysrc')
        dbdao_module = importlib.import_module("dao.DBDao")
        schema_dao = dbdao_module.DBDao()
        schema_dao.drop_schema()
    except Exception as e:
        print(
            f"Failed to drop data characterization results schema '{results_schema}': {e}")
        raise e



async def persist_data_characterization(task, task_run, state, output_folder, dqdresult_dao):
    logger = task_run_logger(task_run, task)
    error_message = None
    result_json = {}
    is_error = state.type == StateType.FAILED
    if is_error:
        is_error = True
        with open(f'{output_folder}/errorReportR.txt', 'rt') as f:
            error_message = f.read()
        logger.error(error_message)
    else:
        # Data_characterization run does not need to insert data into table on completion
        return
    await dqdresult_dao.insert(
        flow_run_id=task_run.flow_run_id,
        result=result_json,
        error=is_error,
        error_message=error_message
    )


async def persist_export_to_ares(task, task_run, state, output_folder, schema_name, dqdresult_dao):
    logger = task_run_logger(task_run, task)
    error_message = None
    result_json = {}
    is_error = state.type == StateType.FAILED
    if is_error:
        error_message = get_export_to_ares_execute_error_message_from_file(output_folder, schema_name)
        logger.error(error_message)
    else:
        result_json = get_export_to_ares_results_from_file(output_folder, schema_name)
    await dqdresult_dao.insert(flow_run_id=task_run.flow_run_id, result=result_json, error=is_error, error_message=error_message)


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
