import os
import sys
import json
import importlib

from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import RemoteFileSystem as RFS

from cohort_generator_plugin.types import CohortGeneratorOptionsType


def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"remotes::install_github('OHDSI/CohortGenerator@0.8.1',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


@flow(log_prints=True, persist_result=True, task_runner=SequentialTaskRunner)
def cohort_generator_plugin(options: CohortGeneratorOptionsType):
    setup_plugin() # To dynamically import helper functions from dataflow-gen
    
    logger = get_run_logger()
    logger.info('Running Cohort Generator')
    database_code = options.database_code
    schema_name = options.schema_name
    vocab_schema_name = options.vocab_schema_name
    cohort_json = options.cohort_json
    dataset_id = options.dataset_id
    description = options.description
    owner = options.owner
    token = options.token
    
    cohort_json_expression = json.dumps(cohort_json.expression)
    cohort_name = cohort_json.name
    
    cohort_definition_id = create_cohort_definition(token,
                                                    dataset_id,
                                                    description,
                                                    owner,
                                                    cohort_json_expression,
                                                    cohort_name)

    create_cohort(database_code,
                  schema_name,
                  cohort_definition_id,
                  cohort_json_expression,
                  cohort_name, 
                  vocab_schema_name)
    
@task(result_storage=RFS.load(os.getenv("DATAFLOW_MGMT__FLOWS__RESULTS_SB_NAME")), 
      result_storage_key="{flow_run.id}_cohort_definition.txt",
      persist_result=True)
def create_cohort_definition(token: str, dataset_id: str, description: str, owner: str, 
                             cohort_json_expression: str, cohort_name: str):
    analytics_svc_api_module = importlib.import_module('api.AnalyticsSvcAPI')
    analytics_svc_api = analytics_svc_api_module.AnalyticsSvcAPI(token)
    result = analytics_svc_api.create_cohort_definition(
        datasetId=dataset_id,
        description=description,
        owner=owner,
        syntax=cohort_json_expression,
        name=cohort_name
    )
    return result


@task
def create_cohort(database_code: str, schema_name: str, cohort_definition_id: int, 
                  cohort_json_expression: str, cohort_name: str, vocab_schema_name: str):
    
    dbutils = importlib.import_module('utils.DBUtils').DBUtils(database_code)
    set_db_driver_env_string = dbutils.set_db_driver_env()
    
    types_module = importlib.import_module("utils.types")
    admin_user = types_module.UserType.ADMIN_USER
    
    set_connection_string = dbutils.get_database_connector_connection_string(
        admin_user,
        None
    )
   
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    robjects = importlib.import_module('rpy2.robjects')
    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(f'''
                .libPaths(c('{r_libs_user_directory}',.libPaths()))
                library('CohortGenerator', lib.loc = '{r_libs_user_directory}')
                {set_db_driver_env_string}
                {set_connection_string}
                cohortJson <- '{cohort_json_expression}'
                schemaName <- '{schema_name}'
                vocabSchemaName <- '{vocab_schema_name}'
                cohortName <- '{cohort_name}'
                cohortId <- '{cohort_definition_id}'
                
                cat("Generating cohort sql from cohort expression from json")
                cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
                options <- CirceR::createGenerateOptions(generateStats = FALSE, vocabularySchema = vocabSchemaName);
                cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = options)
                
                cat("Creating tempoary cohort stats table names")
                cohortTableNames <- list()
                cohortTableNames[["cohortTable"]] <- "cohort"
                cohortTableNames[["cohortInclusionTable"]] <- sprintf("cohort_inclusion_%s", cohortId)
                cohortTableNames[["cohortInclusionResultTable"]] <- sprintf("cohort_inclusion_result_%s", cohortId)
                cohortTableNames[["cohortInclusionStatsTable"]] <- sprintf("cohort_inclusion_stats_%s", cohortId)
                cohortTableNames[["cohortSummaryStatsTable"]] <- sprintf("cohort_summary_stats_%s", cohortId)
                cohortTableNames[["cohortCensorStatsTable"]] <- sprintf("cohort_censor_stats_%s", cohortId)
                
                cat("Creating tempoary cohort stats tables")
                CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                        cohortDatabaseSchema = schemaName,
                                        cohortTableNames = cohortTableNames,
                                        incremental=TRUE)
                                        

                cat("Creating cohorts")
                cohortsToCreate <- CohortGenerator::createEmptyCohortDefinitionSet()
                cohortsToCreate <- rbind(cohortsToCreate, data.frame(cohortId = cohortId,
                                                    cohortName = cohortName, 
                                                    sql = cohortSql,
                                                    stringsAsFactors = FALSE))       
                cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                    cdmDatabaseSchema = schemaName,
                                                    cohortDatabaseSchema = schemaName,
                                                    cohortTableNames = cohortTableNames,
                                                    cohortDefinitionSet = cohortsToCreate)


                cat("Dropping tempoary cohort stats tables")
                CohortGenerator::dropCohortStatsTables(
                connectionDetails = connectionDetails,
                cohortDatabaseSchema = schemaName,
                cohortTableNames = cohortTableNames
                )
        ''')