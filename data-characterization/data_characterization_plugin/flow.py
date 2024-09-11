import os
import sys
import importlib
from functools import partial

from prefect_shell import ShellOperation
from prefect.context import FlowRunContext
from prefect.serializers import JSONSerializer
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from prefect.filesystems import RemoteFileSystem as RFS

from data_characterization_plugin.hooks import *
from data_characterization_plugin.utils.createschema import *
from data_characterization_plugin.utils.types import DCOptionsType, DatabaseDialects, LiquibaseAction



def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"remotes::install_github('OHDSI/Achilles@v1.7.2',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


# on_failure=[drop_data_characterization_schema],
# on_cancellation=[drop_data_characterization_schema]
@flow(log_prints=True, 
      persist_result=True,
      task_runner=SequentialTaskRunner,
      timeout_seconds=3600
      )
def data_characterization_plugin(options: DCOptionsType):
    logger = get_run_logger()
    setup_plugin()

    user_type_module = importlib.import_module("utils.types")
    robjects = importlib.import_module("rpy2.robjects")
    user_dao_module = importlib.import_module("dao.UserDao")
    dbdao_module = importlib.import_module("dao.DBDao")

    schema_name = options.schemaName
    database_code = options.databaseCode
    cdm_version_number = options.cdmVersionNumber
    vocab_schema_name = options.vocabSchemaName
    release_date = options.releaseDate
    results_schema = options.resultsSchema
    flow_name = options.flowName
    changelog_file = options.changelogFile
    use_cache_db = options.use_cache_db

    # comma separated values in a string
    exclude_analysis_ids = options.excludeAnalysisIds

    flow_run_context = FlowRunContext.get().flow_run.dict()
    flow_run_id = str(flow_run_context.get("id"))
    output_folder = f"/output/{flow_run_id}"
    
    admin_user = user_type_module.UserType.ADMIN_USER
    read_user = user_type_module.UserType.READ_USER
    results_schema_dao = dbdao_module.DBDao(use_cache_db=use_cache_db,
                                            database_code=database_code, 
                                            schema_name=results_schema)
    
    user_dao = user_dao_module.UserDao(use_cache_db=use_cache_db,
                                       database_code=database_code, 
                                       schema_name=results_schema)
    dialect = results_schema_dao.get_database_dialect()
    
    match dialect:
        case DatabaseDialects.POSTGRES:
            results_schema = results_schema.lower()
            vocab_schema_name = vocab_schema_name.lower()
            schema_name = schema_name.lower()
        case DatabaseDialects.HANA:
            results_schema = results_schema.upper()
            vocab_schema_name = vocab_schema_name.upper()
            schema_name = schema_name.upper()      
    
    dc_schema = create_data_characterization_schema(
        vocab_schema_name,
        flow_name,
        changelog_file,
        results_schema_dao,
        user_dao
    )

    if dc_schema:
        r_libs_user_directory = os.getenv("R_LIBS_USER")
        
        set_admin_connection_string = results_schema_dao.get_database_connector_connection_string(
            schema_name=results_schema_dao.schema_name,
            user_type=admin_user,
            release_date=release_date
        )
        
        set_read_connection_string = results_schema_dao.get_database_connector_connection_string(
            schema_name=results_schema_dao.schema_name,
            user_type=read_user,
            release_date=release_date
        )       

        dc_status = execute_data_characterization(schema_name=schema_name,
                                                  cdm_version_number=cdm_version_number,
                                                  vocab_schema_name=vocab_schema_name,
                                                  results_schema_dao=results_schema_dao,
                                                  exclude_analysis_ids=exclude_analysis_ids,
                                                  output_folder=output_folder,
                                                  robjects=robjects,
                                                  r_libs_user_directory=r_libs_user_directory,
                                                  set_connection_string=set_admin_connection_string,
                                                  flow_run_id=flow_run_id)

        if dc_status:
            msg = dc_status.get("error_message")
            raise Exception(f"An error occurred while executing data characterization: {msg}")

        execute_export_to_ares(schema_name=schema_name, 
                                                       vocab_schema_name=vocab_schema_name,
                                                       results_schema_dao=results_schema_dao,
                                                       output_folder=output_folder,
                                                       robjects=robjects,
                                                       r_libs_user_directory=r_libs_user_directory,
                                                       set_connection_string=set_read_connection_string,
                                                       flow_run_id=flow_run_id)


def create_data_characterization_schema(vocab_schema_name: str,
                                        flow_name: str,
                                        changelog_file: str,
                                        results_schema_dao,
                                        user_dao):
    try:
        plugin_classpath = get_plugin_classpath(flow_name)
        dialect = results_schema_dao.get_database_dialect()
        tenant_configs = results_schema_dao.tenant_configs
        
        create_schema(results_schema_dao)
        
        # create tables with liquibase
        action = LiquibaseAction.UPDATE
        
        create_tables_wo = run_liquibase_update_task.with_options(
            on_failure=[partial(drop_schema_hook,
                                **dict(schema_dao=results_schema_dao))])
        create_tables_wo(action=action,
                         dialect=dialect,
                         changelog_file=changelog_file,
                         schema_name=results_schema_dao.schema_name,
                         vocab_schema=vocab_schema_name,
                         tenant_configs=tenant_configs,
                         plugin_classpath=plugin_classpath,
                         )


        # enable auditing
        enable_audit_policies = tenant_configs.get("enableAuditPolicies")
        if enable_audit_policies:

            enable_and_create_audit_policies_wo = enable_and_create_audit_policies.with_options(
                on_failure=[partial(drop_schema_hook,
                                    **dict(schema_dao=results_schema_dao))])
            enable_and_create_audit_policies_wo(results_schema_dao)
        else:
            print("Skipping Alteration of system configuration")
            print("Skipping creation of Audit policy for system configuration")
            print(f"Skipping creation of new audit policy for {results_schema_dao.schema_name}")
            
        
        # assign permissions to role/user
        create_and_assign_roles_wo = create_and_assign_roles.with_options(
            on_failure=[partial(drop_schema_hook,
                                **dict(schema_dao=results_schema_dao))])
        create_and_assign_roles_wo(user_dao)

        print(f"Data Characterization results schema '{results_schema_dao.schema_name}' successfully created and privileges assigned!")

    except Exception as e:
        print(e)
        raise e
    else:
        return True


@task(log_prints=True,
      result_storage=RFS.load(os.getenv("DATAFLOW_MGMT__FLOWS__RESULTS_SB_NAME")),
      result_storage_key="{flow_run.id}_persist_data_characterization.json",
      result_serializer=JSONSerializer(),
      persist_result=True)
def execute_data_characterization(schema_name: str,
                                  cdm_version_number: str,
                                  vocab_schema_name: str,
                                  results_schema_dao,
                                  exclude_analysis_ids: str,
                                  output_folder: str,
                                  robjects,
                                  r_libs_user_directory: str,
                                  set_connection_string: str,
                                  flow_run_id: str):
    try:
        logger = get_run_logger()
        threads = os.getenv('ACHILLES_THREAD_COUNT')
        logger.info('Running achilles')
        
        with robjects.conversion.localconverter(robjects.default_converter):
            robjects.r(f'''
                    .libPaths(c('{r_libs_user_directory}',.libPaths()))
                    library('Achilles', lib.loc = '{r_libs_user_directory}')
                    {results_schema_dao.set_db_driver_env()}
                    {set_connection_string}
                    cdmVersion <- '{cdm_version_number}'
                    cdmDatabaseSchema <- '{schema_name}'
                    vocabDatabaseSchema <- '{vocab_schema_name}'
                    resultsDatabaseSchema <- '{results_schema_dao.schema_name}'
                    outputFolder <- '{output_folder}'
                    numThreads <- {threads}
                    createTable <- TRUE
                    sqlOnly <- FALSE
                    excludeAnalysisIds <- c({exclude_analysis_ids})
                    Achilles::achilles( connectionDetails = connectionDetails, cdmVersion = cdmVersion, cdmDatabaseSchema = cdmDatabaseSchema, createTable = createTable, resultsDatabaseSchema = resultsDatabaseSchema, outputFolder = outputFolder, sqlOnly=sqlOnly, numThreads=numThreads, excludeAnalysisIds=excludeAnalysisIds)''')
    except Exception as e:
        logger.error(f"execute_data_characterization task failed")
        result_json = {}
        with open(f'{output_folder}/errorReportR.txt', 'rt') as f:
            error_message = f.read()
        logger.error(error_message)
        
        # drop schema
        logger.info(f"Dropping schema")
        results_schema_dao.drop_schema()

        error_result = {
            "flow_run_id": flow_run_id,
            "result": result_json,
            "error": True,
            "error_message": error_message
        }
        return error_result
        
        
        
    
@task(result_storage=RFS.load(os.getenv("DATAFLOW_MGMT__FLOWS__RESULTS_SB_NAME")),
      result_storage_key="{flow_run.id}_export_to_ares.json",
      result_serializer=JSONSerializer(),
      persist_result=True)
async def execute_export_to_ares(schema_name: str,
                                 vocab_schema_name: str,
                                 results_schema_dao,
                                 output_folder: str,
                                 robjects,
                                 r_libs_user_directory: str,
                                 set_connection_string: str,
                                 flow_run_id: str
                                 ):
    try:
        logger = get_run_logger()
        logger.info('Running exportToAres')
        with robjects.conversion.localconverter(robjects.default_converter):
            robjects.r(f'''
                    .libPaths(c('{r_libs_user_directory}',.libPaths()))
                    library('Achilles', lib.loc = '{r_libs_user_directory}')
                    {results_schema_dao.set_db_driver_env()}
                    {set_connection_string}
                    cdmDatabaseSchema <- '{schema_name}'
                    vocabDatabaseSchema <- '{vocab_schema_name}'
                    resultsDatabaseSchema <- '{results_schema_dao.schema_name}'
                    outputPath <- '{output_folder}'
                    Achilles::exportToAres(
                        connectionDetails = connectionDetails,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        resultsDatabaseSchema = resultsDatabaseSchema,
                        vocabDatabaseSchema = vocabDatabaseSchema,
                        outputPath,
                        reports = c()
                    )
            ''')
            return get_export_to_ares_results_from_file(output_folder, schema_name)
    except Exception as e:
        logger.error(f"execute_export_to_ares task failed")
        error_message = get_export_to_ares_execute_error_message_from_file(output_folder, schema_name)
        logger.error(error_message)
        
        logger.info(f"Dropping Data Characterization results schema '{results_schema_dao.schema_name}'..")
        results_schema_dao.drop_schema()
        
        raise Exception(f"An error occurred while executing export to ares: {error_message}") 

def get_plugin_classpath(flow_name: str) -> str:
    return f'{os.getcwd()}/{flow_name}/'
