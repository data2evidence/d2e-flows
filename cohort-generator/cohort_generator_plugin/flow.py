import os
import sys
import json
import importlib

from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

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
    logger = get_run_logger()
    logger.info('Running Cohort Generator')
    
    setup_plugin() # To dynamically import helper functions from dataflow-gen    
    
    database_code = options.databaseCode
    schema_name = options.schemaName
    vocab_schema_name = options.vocabSchemaName
    cohort_json = options.cohortJson
    dataset_id = options.datasetId
    description = options.description
    owner = options.owner
    token = options.token
    
    analytics_svc_api_module = importlib.import_module('api.AnalyticsSvcAPI')
    analytics_svc_api = analytics_svc_api_module.AnalyticsSvcAPI(token)
    dbutils = importlib.import_module('utils.DBUtils').DBUtils(database_code)
    admin_user = importlib.import_module("utils.types").UserType.ADMIN_USER
    robjects = importlib.import_module('rpy2.robjects')
    
    cohort_json_expression = json.dumps(cohort_json.expression)
    cohort_name = cohort_json.name
    
    cohort_definition_id = create_cohort_definition(analytics_svc_api,
                                                    dataset_id,
                                                    description,
                                                    owner,
                                                    cohort_json_expression,
                                                    cohort_name)

    create_cohort(dbutils,
                  admin_user,
                  schema_name,
                  cohort_definition_id,
                  cohort_json_expression,
                  cohort_name, 
                  vocab_schema_name,
                  robjects)
    
@task(log_prints=True)
def create_cohort_definition(analytics_svc_api, dataset_id: str, description: str, owner: str, 
                             cohort_json_expression: str, cohort_name: str):

    result = analytics_svc_api.create_cohort_definition(
        datasetId=dataset_id,
        description=description,
        owner=owner,
        syntax=cohort_json_expression,
        name=cohort_name
    )
    return result


@task(log_prints=True)
def create_cohort(dbutils, admin_user, schema_name: str, cohort_definition_id: int, 
                  cohort_json_expression: str, cohort_name: str, vocab_schema_name: str, 
                  robjects):
    
    set_db_driver_env_string = dbutils.set_db_driver_env()
    
    set_connection_string = dbutils.get_database_connector_connection_string(
        admin_user,
        None
    )
   
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    
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