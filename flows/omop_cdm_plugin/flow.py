from rpy2 import robjects
from functools import partial
from datetime import datetime

from prefect.variables import Variable
from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from flows.omop_cdm_plugin.types import *
from flows.omop_cdm_plugin.utils import *

from shared_utils.types import UserType
from shared_utils.dao.DBDao import DBDao
from shared_utils.dao.UserDao import UserDao
from shared_utils.create_dataset_tasks import *
from shared_utils.update_dataset_metadata import *
from shared_utils.api.PortalServerAPI import PortalServerAPI


def setup_plugin(release_version):
    r_libs_user_directory = Variable.get("r_libs_user").value
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"remotes::install_github('OHDSI/CommonDataModel@{release_version}',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\""
            ]).run()
    else:
        raise ValueError("Prefect variable: 'r_libs_user' is empty.")

@flow(log_prints=True, task_runner=SequentialTaskRunner)
def omop_cdm_plugin(options: OmopCDMPluginOptions):
    match options.flow_action_type:
        case FlowActionType.CREATE_DATA_MODEL:
            create_omop_cdm_dataset(options)
        case FlowActionType.GET_VERSION_INFO:
            update_dataset_metadata(options)    
    

def create_omop_cdm_dataset(options: OmopCDMPluginOptions):   
    logger = get_run_logger()
    database_code = options.database_code
    schema_name = options.schema_name
    vocab_schema = options.vocab_schema
    cdm_version = options.cdm_version
    use_cache_db = options.use_cache_db
    release_version =  options.release_version

    try:
        setup_plugin(release_version)
        
        omop_cdm_dao = DBDao(use_cache_db=use_cache_db,
                             database_code=database_code, 
                             schema_name=schema_name)
        

        userdao = UserDao(use_cache_db=use_cache_db,
                          database_code=database_code, 
                          schema_name=schema_name)
        
        create_schema_task(omop_cdm_dao)

        # Run CommonDataModel package to create tables 
        # With drop schema hook on failure
        create_cdm_tables_wo = create_cdm_tables.with_options(
            on_failure=[partial(drop_schema_hook,
                                **dict(schema_dao=omop_cdm_dao))]
        )
        create_cdm_tables_wo(omop_cdm_dao, cdm_version, logger)
        
        # Grant permissions
        create_and_assign_roles_wo = create_and_assign_roles_task.with_options(
            on_failure=[partial(drop_schema_hook,
                                **dict(schema_dao=omop_cdm_dao))]
        )        
        
        create_and_assign_roles_wo(
            userdao=userdao
        )

        if cdm_version == CDMVersion.OMOP54:
            # v5.3 does not have cohort table
            # Grant write cohort and cohort_definition table privileges to read role
            grant_cohort_write_privileges_wo = grant_cohort_write_privileges.with_options(
                on_failure=[partial(drop_schema_hook,
                                    **dict(schema_dao=omop_cdm_dao))]
            )        
            grant_cohort_write_privileges_wo(userdao, logger)
        
        
        if schema_name != vocab_schema:
            # Insert CDM Version
            vocab_schema_dao = DBDao(use_cache_db=use_cache_db,
                                     database_code=database_code, 
                                     schema_name=vocab_schema)

            insert_cdm_version_wo = insert_cdm_version.with_options(
                on_failure=[partial(drop_schema_hook,
                                    **dict(schema_dao=omop_cdm_dao))]
            )  
            insert_cdm_version_wo(
                cdm_version=cdm_version,
                schema_dao=omop_cdm_dao,
                vocab_schema_dao=vocab_schema_dao
            )
        else:
            # If newly created schema is also the vocab schema
            # Todo: Add insertion of cdm version to update flow
            logger.info("Skipping insertion of 'CDM Version. Please load vocabulary data first.")
    except Exception as e:
        logger.error(e)
        raise e


@task(log_prints=True)
def create_cdm_tables(dbdao, cdm_version: str, logger):
    # currently only supports pg dialect
    r_libs_user_directory = Variable.get("r_libs_user").value

    admin_user =  UserType.ADMIN_USER
    set_connection_string = dbdao.get_database_connector_connection_string(
        user_type=admin_user
    )
    
    logger.info(f"Running CommonDataModel version '{cdm_version}' on schema '{dbdao.schema_name}' in database '{dbdao.database_code}'")
    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(
            f'''
            .libPaths(c('{r_libs_user_directory}',.libPaths()))
            library('CommonDataModel', lib.loc = '{r_libs_user_directory}')
            {set_connection_string}
            cdm_version <- "{cdm_version}"
            schema_name <- "{dbdao.schema_name}"
            CommonDataModel::executeDdl(connectionDetails = connectionDetails, cdmVersion = cdm_version, cdmDatabaseSchema = schema_name, executeDdl = TRUE, executePrimaryKey = TRUE, executeForeignKey = FALSE)
            '''
        )
    logger.info(f"Succesfully ran CommonDataModel version '{cdm_version}' on schema '{dbdao.schema_name}' in database '{dbdao.database_code}'")


@task(log_prints=True)
def grant_cohort_write_privileges(userdao: UserDao, logger):
    logger.info(f"Granting cohort write privileges to '{userdao.read_role}' role")
    userdao.grant_cohort_write_privileges(userdao.read_role)


def update_dataset_metadata(options: OmopCDMPluginOptions):
    logger = get_run_logger()
    dataset_list = options.datasets
    token = options.token
    use_cache_db = options.use_cache_db
    
    if (dataset_list is None) or (len(dataset_list) == 0):
        logger.debug("No datasets fetched from portal")
    else:
        logger.info(f"Successfully fetched {len(dataset_list)} datasets from portal")
        for dataset in dataset_list:
            get_and_update_attributes(token, dataset, use_cache_db)


@task(log_prints=True)
def get_and_update_attributes(token: str, dataset: dict, use_cache_db: bool):
    logger = get_run_logger()

    try:
        dataset_id = dataset.get("id")
        database_code = dataset.get("databaseCode")
        schema_name = dataset.get("schemaName")
    except KeyError as ke:
        missing_key = ke.args[0]
        logger.error(f"'{missing_key} not found in dataset'")
    else:
        dbdao = DBDao(use_cache_db=use_cache_db,
                      database_code=database_code, 
                      schema_name=schema_name)
        portal_server_api = PortalServerAPI(token)
        
        # check if schema exists
        schema_exists = dbdao.check_schema_exists()
        if schema_exists is False:
            error_msg = f"Schema '{schema_name}' does not exist in db {database_code} for dataset id '{dataset_id}'"
            logger.error(error_msg)
            portal_server_api.update_dataset_attributes_table(dataset_id, "schema_version", error_msg)
            portal_server_api.update_dataset_attributes_table(dataset_id, "latest_schema_version", error_msg)
        else:
            
            
            # update last created_date with cdm_release_date or error msg
            update_entity_value(
                portal_server_api=portal_server_api,
                dataset_id=dataset_id,
                dbdao=dbdao,
                table_name="cdm_source",
                column_name="cdm_release_date",
                entity_name="created_date",
                logger=logger
                )
            
            # update updated_date with cdm_release_date or error msg
            update_entity_value(
                portal_server_api=portal_server_api,
                dataset_id=dataset_id,
                dbdao=dbdao,
                table_name="cdm_source",
                column_name="cdm_release_date",
                entity_name="updated_date",
                logger=logger
                )
            
            # update patient count or error msg
            update_entity_distinct_count(
                portal_server_api=portal_server_api,
                dataset_id=dataset_id,
                dbdao=dbdao,
                table_name="person",
                column_name="person_id",
                entity_name="patient_count",
                logger=logger
                )
            
            # update entity_count_distribution or error msg
            entity_count_distribution = update_entity_count_distribution(
                portal_server_api=portal_server_api,
                dataset_id=dataset_id,
                dbdao=dbdao,
                logger=logger
            )
            
            # update total_entity_count or error msg
            update_total_entity_count(
                portal_server_api=portal_server_api,
                dataset_id=dataset_id,
                entity_count_distribution=entity_count_distribution,
                logger=logger
            )

            # update cdm version or error msg
            cdm_version = update_entity_distinct_count(
                portal_server_api=portal_server_api,
                dataset_id=dataset_id,
                dbdao=dbdao,
                table_name="cdm_source",
                column_name="cdm_version",
                entity_name="cdm_version",
                logger=logger
                )

            try:
                # update schema version or error msg
                schema_version = RELEASE_VERSION_MAPPING.get(cdm_version)
                portal_server_api.update_dataset_attributes_table(dataset_id, "schema_version", schema_version)
            except Exception as e:
                logger.error(f"Failed to update attribute 'schema_version' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'schema_version' for dataset '{dataset_id}' with value '{schema_version}'")


            try:
                # update latest schema version or error msg
                schema_version = RELEASE_VERSION_MAPPING.get(cdm_version)
                latest_schema_version = RELEASE_VERSION_MAPPING.get("5.4")
                portal_server_api.update_dataset_attributes_table(dataset_id, "latest_schema_version", latest_schema_version)
            except Exception as e:
                logger.error(f"Failed to update attribute 'latest_schema_version' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'latest_schema_version' for dataset '{dataset_id}' with value '{latest_schema_version}'")


            update_metadata_last_fetched_date(
                portal_server_api=portal_server_api,
                dataset_id=dataset_id,
                logger=logger
            )