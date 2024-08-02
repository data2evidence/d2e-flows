import os
import sys
import json
import importlib
from functools import partial
from datetime import datetime


from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner


from omop_cdm_plugin.types import *
from omop_cdm_plugin.utils import *
from omop_cdm_plugin.hooks import *
from omop_cdm_plugin.createdatamodel import *


def setup_plugin(release_version):
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    # force=TRUE for fresh install everytime flow is run
    if (r_libs_user_directory):
        ShellOperation(
            commands=[
                f"Rscript -e \"remotes::install_github('OHDSI/CommonDataModel@{release_version}',quiet=FALSE,upgrade='never',force=TRUE, dependencies=FALSE, lib='{r_libs_user_directory}')\""
            ]).run()
    else:
        raise ValueError("Environment variable: 'R_LIBS_USER' is empty.")


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
    data_model = options.data_model
    cdm_version = options.cdm_version
    release_version =  options.release_version

    try:
        setup_plugin(release_version) # To dynamically import helper functions from dataflow-gen
        types_module = importlib.import_module('utils.types')
        admin_user = types_module.UserType.ADMIN_USER

        # import helper function to create schema
        dbdao_module = importlib.import_module('dao.DBDao')
        omop_cdm_dao = dbdao_module.DBDao(database_code, schema_name, admin_user)
        
        # import helper function to create roles
        userdao_module = importlib.import_module('dao.UserDao')
        userdao = userdao_module.UserDao(database_code, schema_name, admin_user)
        
        # import helper function to get database connection
        dbutils_module = importlib.import_module('utils.DBUtils')
        dbutils = dbutils_module.DBUtils(database_code)
        tenant_configs = dbutils.extract_database_credentials()
        
        create_schema(omop_cdm_dao, logger)

        # Run CommonDataModel package to create tables 
        # With drop schema hook on failure
        create_cdm_tables_wo = create_cdm_tables.with_options(
            on_failure=[partial(drop_schema_hook,
                                **dict(schema_dao=omop_cdm_dao))]
        )
        create_cdm_tables_wo(database_code, schema_name, cdm_version, admin_user, logger)
        
        # Grant permissions
        create_and_assign_roles_wo = create_and_assign_roles.with_options(
            on_failure=[partial(drop_schema_hook,
                                **dict(schema_dao=omop_cdm_dao))]
        )        
        
        create_and_assign_roles_wo(
            userdao=userdao,
            tenant_configs=tenant_configs,
            cdm_version=cdm_version
        )
        
        if schema_name != vocab_schema:
            # Insert CDM Version
            vocab_schema_dao = dbdao_module.DBDao(database_code, vocab_schema, admin_user)
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
def create_schema(dbdao, logger):
    # currently only supports pg dialect
    schema_exists = dbdao.check_schema_exists()
    if schema_exists == False:
        dbdao.create_schema()
    else:
        error_msg = f"Schema {dbdao.schema_name} already exists in database {dbdao.database_code}"
        logger.error(error_msg)
        raise Exception(error_msg)


@task(log_prints=True)
def create_cdm_tables(database_code, schema_name, cdm_version, user, logger):
    # currently only supports pg dialect
    r_libs_user_directory = os.getenv("R_LIBS_USER")
    robjects = importlib.import_module('rpy2.robjects')
    dbutils_module = importlib.import_module('utils.DBUtils')
    dbutils = dbutils_module.DBUtils(database_code)
    
    set_connection_string = dbutils.get_database_connector_connection_string(user)
    print(f"set_connection_string is {set_connection_string}")
    
    logger.info(f"Running CommonDataModel version '{cdm_version}' on schema '{schema_name}' in database '{database_code}'")
    with robjects.conversion.localconverter(robjects.default_converter):
        robjects.r(
            f'''
            .libPaths(c('{r_libs_user_directory}',.libPaths()))
            library('CommonDataModel', lib.loc = '{r_libs_user_directory}')
            {set_connection_string}
            cdm_version <- "{cdm_version}"
            schema_name <- "{schema_name}"
            CommonDataModel::executeDdl(connectionDetails = connectionDetails, cdmVersion = cdm_version, cdmDatabaseSchema = schema_name, executeDdl = TRUE, executePrimaryKey = TRUE, executeForeignKey = FALSE)
            '''
        )
    logger.info(f"Succesfully ran CommonDataModel version '{cdm_version}' on schema '{schema_name}' in database '{database_code}'")


def update_dataset_metadata(options: OmopCDMPluginOptions):
    logger = get_run_logger()
    dataset_list = options.datasets
    token = options.token
    if (dataset_list is None) or (len(dataset_list) == 0):
        logger.debug("No datasets fetched from portal")
    else:
        logger.info(f"Successfully fetched {len(dataset_list)} datasets from portal")
        for dataset in dataset_list:
            get_and_update_attributes(token, dataset)

@task(log_prints=True)
def get_and_update_attributes(token: str, dataset: dict):
    logger = get_run_logger()

    sys.path.append('/app/pysrc')
    dbdao_module = importlib.import_module('dao.DBDao')
    types_modules = importlib.import_module('utils.types')
    portal_server_api_module = importlib.import_module('api.PortalServerAPI')
    
    admin_user = types_modules.UserType.ADMIN_USER
        
    try:
        dataset_id = dataset.get("id")
        database_code = dataset.get("databaseCode")
        schema_name = dataset.get("schemaName")
    except KeyError as ke:
        missing_key = ke.args[0]
        logger.error(f"'{missing_key} not found in dataset'")
    else:
        dbdao = dbdao_module.DBDao(database_code, schema_name, admin_user) 
        portal_server_api = portal_server_api_module.PortalServerAPI(token)
        
        # check if schema exists
        schema_exists = dbdao.check_schema_exists()
        if schema_exists is False:
            error_msg = f"Schema '{schema_name}' does not exist in db {database_code} for dataset id '{dataset_id}'"
            logger.error(error_msg)
            portal_server_api.update_dataset_attributes_table(dataset_id, "schema_version", error_msg)
            portal_server_api.update_dataset_attributes_table(dataset_id, "latest_schema_version", error_msg)
        else:
            
            try:
                # update with data model creation date and last updated date
                cdm_release_date = get_cdm_release_date(dbdao, logger)
                portal_server_api.update_dataset_attributes_table(dataset_id, "created_date", cdm_release_date)
                portal_server_api.update_dataset_attributes_table(dataset_id, "updated_date", cdm_release_date)
            except Exception as e:
                logger.error(
                    f"Failed to update attribute 'created_date', 'updated_date' for dataset id '{dataset_id}' with value '{cdm_release_date}' : {e}")
            else:
                logger.info(
                    f"Updated attribute 'created_date', 'updated_date' for dataset id '{dataset_id}' with value '{cdm_release_date}'")


            try:
                # update patient count or error msg
                patient_count = get_patient_count(dbdao, logger)
                portal_server_api.update_dataset_attributes_table(dataset_id, "patient_count", patient_count)
            except Exception as e:
                logger.error(f"Failed to update attribute 'patient_count' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'patient_count' for dataset '{dataset_id}' with value '{patient_count}'")


            try:
                # update get_entity_count_distribution or error msg
                entity_count_distribution = get_entity_count_distribution(dbdao, logger)
                portal_server_api.update_dataset_attributes_table(dataset_id, "entity_count_distribution", json.dumps(entity_count_distribution))
            except Exception as e:
                logger.error(f"Failed to update attribute 'entity_count_distribution' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'entity_count_distribution' for dataset '{dataset_id}' with value '{json.dumps(entity_count_distribution)}'")


            try:
                # update total_entity_count or error msg
                total_entity_count = get_total_entity_count(entity_count_distribution, logger)
                portal_server_api.update_dataset_attributes_table(dataset_id, "entity_count", total_entity_count)
            except Exception as e:
                logger.error(f"Failed to update attribute 'entity_count' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'entity_count' for dataset '{dataset_id}' with value '{total_entity_count}'")


            try:
                # update cdm version or error msg
                cdm_version = get_cdm_version(dbdao, logger)    
                portal_server_api.update_dataset_attributes_table(dataset_id, "version", cdm_version)
            except Exception as e:
                logger.error(f"Failed to update attribute 'cdm_version' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'cdm_version' for dataset '{dataset_id}' with value '{cdm_version}'")


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


            try:
                # update last fetched metadata date
                metadata_last_fetch_date = datetime.now().strftime('%Y-%m-%d')
                portal_server_api.update_dataset_attributes_table(dataset_id, "metadata_last_fetch_date", metadata_last_fetch_date)
            except Exception as e:
                logger.error(f"Failed to update attribute 'metadata_last_fetch_date' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'metadata_last_fetch_date' for dataset '{dataset_id}' with value '{metadata_last_fetch_date}'")
