import os
import sys
import json
import importlib

from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from strategus_plugin.types import StrategusAnalysisType, StrategusOptionsType


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
def strategus_plugin(analysis_spec: StrategusAnalysisType, options: StrategusOptionsType):
    setup_plugin() # To dynamically import helper functions from dataflow-gen
    
    logger = get_run_logger()
    work_schema = options.workSchema
    cdm_schema = options.cdmSchema
    database_code = options.databaseCode

    dbutils = importlib.import_module('utils.DBUtils').DBUtils(database_code)
    admin_user = importlib.import_module('utils.types').UserType.ADMIN_USER
    robjects = importlib.import_module('rpy2.robjects')
    connection_details_string = dbutils.get_database_connector_connection_string(admin_user)

    shared_resources = json.dumps(analysis_spec['sharedResources'])
    module_specifications = json.dumps(analysis_spec['moduleSpecifications'])

    shared_resources = shared_resources.replace('\\r', '')
    shared_resources = shared_resources.replace('\\n', '')
    shared_resources = shared_resources.replace('\\t', '')
    shared_resources = shared_resources.replace('\\\"', '\\\\\\"')
    
    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(f'''
            library(jsonlite)
            library(Strategus)
            
            # Default keyring system is required, however it is unused in the context of ALP
            if(!"system" %in% keyring::keyring_list()){{
                # to check whether password can be avoided or not
                keyring::keyring_create(keyring = 'system', password = 'dummy')  
            }}

            {connection_details_string}
            print("{logger.info('Running Strategus')}")
            # TODO: to check STRATEGUS_KEYRING_PASSWORD
            Sys.setenv(STRATEGUS_KEYRING_PASSWORD = "dummy")
            Sys.setenv("INSTANTIATED_MODULES_FOLDER" = "/tmp/StrategusInstantiatedModules")

            storeConnectionDetails(
                connectionDetails = connectionDetails,
                connectionDetailsReference = 'dbconnection'
            )

            executionSettings <- createCdmExecutionSettings(connectionDetailsReference = 'dbconnection',
                                                            workDatabaseSchema = '{work_schema}',
                                                            cdmDatabaseSchema = '{cdm_schema}',
                                                            workFolder = "/tmp/strategusWork",
                                                            resultsFolder = "/tmp/strategusOutput")

            sharedResources <- '{shared_resources}'
            moduleSpecifications <- '{module_specifications}'
            sharedResources <- fromJSON(sharedResources, simplifyVector = FALSE, unexpected.escape="keep")
            moduleSpecifications <- fromJSON(moduleSpecifications, simplifyVector = FALSE, unexpected.escape="keep")

            analysisSpecifications <- createEmptyAnalysisSpecificiations()
            for(o in sharedResources) {{
                print(o)
                class(o) <- o$attr_class
                # o[[attr_class]] <- NULL
                analysisSpecifications <- addSharedResources(analysisSpecifications, o)
            }}

            for(o in moduleSpecifications) {{
                class(o) <- o$attr_class
                # o[[attr_class]] <- NULL
                analysisSpecifications <- addModuleSpecifications(analysisSpecifications, o)
            }}

            print("{logger.info('Executing Strategus::execute')}")
            execute(analysisSpecifications = analysisSpecifications, executionSettings = executionSettings)
        ''')