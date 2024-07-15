import os
import sys
import importlib

from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from omop_cdm_plugin.types import OmopCDMPluginOptions


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"remotes::install_github('OHDSI/CommonDataModel@v5.4.1',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, task_runner=SequentialTaskRunner)
def omop_cdm_plugin(options: OmopCDMPluginOptions):
    setup_plugin() # To dynamically import helper functions from dataflow-gen
    dbdao_module = importlib.import_module('dao.DBDao')
    types_module = importlib.import_module('utils.types')
    admin_user = types_module.UserType.ADMIN_USER
    #robjects_module = importlib.import_module('rpy2')
    
    logger = get_run_logger()

    database_code = options.database_code
    schema_name = options.schema_name
    cdm_version = options.cdm_version

    omop_cdm_dao = dbdao_module.DBDao(database_code, schema_name, admin_user)

    schema_exists = check_schema_exists(omop_cdm_dao)
    if schema_exists:
        schema_empty = check_empty_schema(omop_cdm_dao)
    else:
        create_schema(omop_cdm_dao)
        schema_exists = True
        schema_empty = True

    if schema_exists & schema_empty:
        create_cdm_tables(database_code, schema_name, cdm_version, admin_user)
    else:
        get_run_logger().error(
            f"Schema {schema_name} already exists in database with code:{database_code} and is not empty!")


@task(log_prints=True)
def check_empty_schema(omop_cdm_dao):
    # currently only supports pg dialect
    return omop_cdm_dao.check_empty_schema()


@task(log_prints=True)
def check_schema_exists(omop_cdm_dao):
    # currently only supports pg dialect
    return omop_cdm_dao.check_schema_exists()


@task(log_prints=True)
def create_schema(omop_cdm_dao):
    # currently only supports pg dialect
    return omop_cdm_dao.create_schema()


@task(log_prints=True)
def create_cdm_tables(database_code, schema_name, cdm_version, user):
    # currently only supports pg dialect
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    robjects = importlib.import_module('rpy2.robjects')
    dbutils_module = importlib.import_module('utils.DBUtils')
    dbutils = dbutils_module.DBUtils(database_code)
    set_connection_string = dbutils.get_database_connector_connection_string(user)
    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(
            f'''
            .libPaths(c('{r_libs_user_directory}',.libPaths()))
            library('CommonDataModel', lib.loc = '{r_libs_user_directory}')
            {set_connection_string}
            cdm_version <- "{cdm_version}"
            schema_name <- "{schema_name}"
            CommonDataModel::executeDdl(connectionDetails = connectionDetails, cdmVersion = cdm_version, cdmDatabaseSchema = schema_name)
            '''
        )
