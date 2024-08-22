import os
import sys
import json
import importlib
from functools import partial

from prefect_shell import ShellOperation
from prefect.serializers import JSONSerializer
from prefect import flow, task, get_run_logger
from prefect.server.schemas.states import StateType
from prefect.logging.loggers import task_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import RemoteFileSystem as RFS
from prefect.context import TaskRunContext, FlowRunContext

from dqd_plugin.types import DqdOptionsType


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    # Install dqd R package from plugin
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"install.packages('./dqd_plugin/DataQualityDashboard-2.6.0', lib='{r_libs_user_directory}', repos = NULL, type='source')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, task_runner=SequentialTaskRunner, timeout_seconds=3600)
def dqd_plugin(options: DqdOptionsType):
    setup_plugin() #dqd_flow_module = importlib.import_module('flows.alp_dqd.flow')
    schema_name = options.schemaName
    database_code = options.databaseCode
    cdm_version_number = options.cdmVersionNumber
    vocab_schema_name = options.vocabSchemaName
    release_date = options.releaseDate

    if options.cohortDefinitionId:
        cohort_definition_id = f"c({options.cohortDefinitionId})"
    else:
        cohort_definition_id = "c()"

    if options.checkNames:
        # Wrap each value in checkNames in single quotes
        check_names = [
            f"'{check_name}'" for check_name in options.checkNames]
        # convert to comma separated string
        check_names = f"c({','.join(check_names)})"
    else:
        check_names = "c()"

    if options.cohortDatabaseSchema:
        cohort_database_schema = options.cohortDatabaseSchema
    else:
        cohort_database_schema = schema_name

    if options.cohortTableName:
        cohort_table_name = options.cohortTableName
    else:   
        cohort_table_name = "cohort"

    flow_run_context = FlowRunContext.get().flow_run.dict()
    flow_run_id = str(flow_run_context.get("id"))
    output_folder = f'/output/{flow_run_id}'
    execute_dqd(schema_name,
                   database_code,
                   cdm_version_number,
                   vocab_schema_name,
                   release_date,
                   cohort_definition_id,
                   output_folder,
                   check_names,
                   cohort_database_schema,
                   cohort_table_name)
    
@task(result_storage=RFS.load(os.getenv("DATAFLOW_MGMT__FLOWS__RESULTS_SB_NAME")), 
      result_storage_key="{flow_run.id}_dqd.json",
      result_serializer=JSONSerializer(),
      persist_result=True)
def execute_dqd(
    schema_name: str,
    database_code: str,
    cdm_version_number: str,
    vocab_schema_name: str,
    release_date: str,
    cohort_definition_id: str,
    output_folder: str,
    check_names: str,
    cohort_database_schema: str,
    cohort_table_name: str,
):
    logger = get_run_logger()

    threads = os.getenv('DQD_THREAD_COUNT')
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    
    
    dbutils_module = importlib.import_module('utils.DBUtils')
    types_module = importlib.import_module('utils.types')
    read_user = types_module.UserType.READ_USER
    dbutils = dbutils_module.DBUtils(database_code)
    
    set_db_driver_env = dbutils.set_db_driver_env()
    set_read_user_connection = dbutils.get_database_connector_connection_string(schema_name, vocab_schema_name, release_date)
    
    logger.info(f"set_read_user_connection is {set_read_user_connection}")

    logger.info(f'''Running DQD with input parameters:
                    schemaName: {schema_name},
                    databaseCode: {database_code},
                    cdmVersionNumber: {cdm_version_number},
                    vocabSchemaName: {vocab_schema_name},
                    releaseDate: {release_date},
                    cohortDefinitionId: {cohort_definition_id},
                    outputFolder: {output_folder},
                    checkNames: {check_names}
                    cohortDatabaseSchema: {cohort_database_schema}
                    cohortTableName: {cohort_table_name}
                '''
                )
    # raise Exception("test stop")
    robjects = importlib.import_module('rpy2.robjects')
    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(f'''
                {set_db_driver_env}
                {set_read_user_connection}
                cdmDatabaseSchema <- '{schema_name}'
                vocabDatabaseSchema <- '${vocab_schema_name}'
                resultsDatabaseSchema <- '{schema_name}'
                cdmSourceName <- '{schema_name}'
                numThreads <- {threads}
                sqlOnly <- FALSE
                outputFolder <- '{output_folder}'
                outputFile <- '{schema_name}.json'
                writeToTable <- FALSE
                verboseMode <- TRUE
                checkLevels <- c('TABLE','FIELD','CONCEPT')
                checkNames <- {check_names}
                cohortDefinitionId <- {cohort_definition_id}
                cdmVersion <- '{cdm_version_number}'
                cohortDatabaseSchema <- '{cohort_database_schema}'
                cohortTableName <- '{cohort_table_name}'

                # Set r_libs_user_directory to be the priority for packages to be loaded
                .libPaths('{r_libs_user_directory}')

                # Run executeDqChecks
                DataQualityDashboard::executeDqChecks(connectionDetails = connectionDetails,cdmDatabaseSchema = cdmDatabaseSchema,resultsDatabaseSchema = resultsDatabaseSchema,cdmSourceName = cdmSourceName,numThreads = numThreads,sqlOnly = sqlOnly,outputFolder = outputFolder,outputFile = outputFile,verboseMode = verboseMode,writeToTable = writeToTable,checkLevels = checkLevels,checkNames = checkNames,cdmVersion = cdmVersion, cohortDefinitionId = cohortDefinitionId, cohortDatabaseSchema = cohortDatabaseSchema, cohortTableName = cohortTableName)
        ''')
    with open(f'{output_folder}/{schema_name}.json', 'rt') as f:
            return json.loads(f.read())


if __name__ == "__main__":
    try:
        execute_dqd({
            "schemaName": "schemaName",
            "cdmVersionNumber": '5.4',
            "threads": 1
        })
        sys.exit(0)
    except Exception as e:
        print(e)