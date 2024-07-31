import os
import sys
import json
import importlib
import pandas as pd
from time import time
from datetime import datetime

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from datamart_plugin.types import *
from datamart_plugin.const import *
from datamart_plugin.utils import *

def setup_plugin():
    # Setup plugin by adding path to python flow source so that modules from app/pysrc in dataflow-gen-agent container can be imported dynamically
    sys.path.append('/app/pysrc')



@flow(log_prints=True, task_runner=SequentialTaskRunner)
def datamart_plugin(options: CreateDatamartOptions):
    match options.flow_action_type:
        case DatamartFlowAction.CREATE_SNAPSHOT | DatamartFlowAction.CREATE_PARQUET_SNAPSHOT:
            create_datamart(options) 
        case DatamartFlowAction.GET_VERSION_INFO:
            update_dataset_metadata(options)   



def create_datamart(options: CreateDatamartOptions):
    logger = get_run_logger()
    setup_plugin() # To dynamically import helper functions from dataflow-gen
    
    dbdao_module = importlib.import_module('dao.DBDao')
    types_module = importlib.import_module('utils.types')
    admin_user = types_module.UserType.ADMIN_USER

    database_code = options.database_code
    source_schema = options.source_schema # schema to copy from
    target_schema = options.schema_name # schema to copy to
    datamart_action = options.flow_action_type
    snapshot_copy_config = options.snapshot_copy_config
    
    source_dbdao = dbdao_module.DBDao(database_code, source_schema, admin_user)
    target_dbdao = dbdao_module.DBDao(database_code, target_schema, admin_user)
    
    source_schema_exists = source_dbdao.check_schema_exists()
    if not source_schema_exists:
        raise ValueError(f"Source schema '{source_schema}' does not exist in database '{database_code}'")
    
    
    try:
        _, failed_tables = copy_schema(datamart_action,
                                       snapshot_copy_config,
                                       source_dbdao,
                                       target_dbdao)
        
        
        if len(failed_tables) > 0:
            error_message = f"The following tables has failed datamart creation: {failed_tables}"
            logger.error(error_message)
            raise Exception(error_message)
    except Exception as err:
        error_message = f"Schema: {target_schema} created successful, but failed to load data with Error: {err}"
        logger.error(error_message)
        if datamart_action == DatamartFlowAction.CREATE_SNAPSHOT:
            logger.info(f"Cleaning up schema '{target_schema}'")
            cleanup_target_schema(target_dbdao)
            logger.info(f"Successfully dropped schema '{target_schema}'")
            raise Exception(error_message) from err
    else:
        logger.info(
            f"{target_schema} schema created and loaded from source schema: {source_schema} with configuration {snapshot_copy_config}")

        if datamart_action == DatamartFlowAction.CREATE_SNAPSHOT:
            try:
                userdao_module = importlib.import_module('dao.UserDao')
                userdao = userdao_module.UserDao(database_code, target_schema, admin_user)
                logger.info(f"Granting read privileges to datamart schema '{target_dbdao.database_code}.{target_dbdao.schema_name}'..")                
                create_and_assign_roles(userdao=userdao)
                logger.info(f"Successfully granted read privileges to datamart schema '{target_dbdao.database_code}.{target_dbdao.schema_name}'!")
            except Exception as err:
                error_message = f"Failed to grant read privileges to datamart schema '{target_dbdao.database_code}.{target_dbdao.schema_name}'!"
                logger.error(error_message)
                logger.info(f"Cleaning up schema '{target_schema}'")
                cleanup_target_schema(target_dbdao)
                logger.info(f"Successfully dropped schema '{target_schema}'")
                raise Exception(error_message) from err

@task(log_prints=True)
def copy_schema(datamart_action: str,
                snapshot_copy_config: DatamartCopyConfig,
                source_dbdao,
                target_dbdao):
    logger = get_run_logger()
    date_filter, table_filter, patient_filter = parse_datamart_copy_config(snapshot_copy_config)
    tables_to_copy = get_tables_to_copy(source_dbdao, table_filter)
    
    successful_tables: list[str] = []
    failed_tables: list[str] = []
    

    if datamart_action == DatamartFlowAction.CREATE_SNAPSHOT:
        target_schema_exists = target_dbdao.check_schema_exists()
        if target_schema_exists:
            raise ValueError(f"Datamart schema '{target_dbdao.schema_name}' already exists in database '{target_dbdao.database_code}'!")
        target_dbdao.create_schema()
    

    for table in tables_to_copy:
        # get the columns to copy for each table
        columns_to_copy = get_columns_to_copy(source_dbdao, table, table_filter)
        base_config_table = BASE_CONFIG_LIST.get(table, {})
        timestamp_column = base_config_table.get("timestamp_column", "")
        person_id_column = base_config_table.get("person_id_column", "")


        select_statement = source_dbdao.create_datamart_select_statement(
            table_name=table,
            columns_to_copy=columns_to_copy,
            patient_filter=patient_filter,
            person_id_column=person_id_column,
            date_filter=date_filter,
            timestamp_column=timestamp_column
        )
        

        match datamart_action:
            case DatamartFlowAction.CREATE_SNAPSHOT:
                try:
                    
                    # copy from source schema to target schema
                    rows_copied = create_copy_table(source_dbdao, target_dbdao.schema_name, table, select_statement)
                except Exception as err:
                    logger.error(f"""Datamart copying failed from {source_dbdao.schema_name} to {
                        target_dbdao.schema_name} for table: {table} with Error:{err}""")
                    failed_tables.append(table)
                else:
                    logger.info(f"""Succesfully copied {rows_copied} rows from {
                        source_dbdao.schema_name} to {target_dbdao.schema_name} for table: {table}""")
                    successful_tables.append(table)
            case DatamartFlowAction.CREATE_PARQUET_SNAPSHOT:
                try:
                    datamart_df = source_dbdao.create_dataframe_from_query(select_statement)
                    upload_df_as_parquet(target_dbdao.schema_name, table, datamart_df, logger)
                except Exception as err:
                    logger.error(f"""Datamart parquet creation failed for {source_dbdao.schema_name} to {
                        target_dbdao.schema_name} for table: {table} with Error:{err}""")
                    failed_tables.append(table)
                else:
                    logger.info(f"""Succesfully created parquet file for {source_dbdao.schema_name} to {
                        target_dbdao.schema_name} for table: {table}""")
                    successful_tables.append(table)
                    
    logger.info(f"Successful Tables: {successful_tables}")
    logger.info(f"Failed Tables: {failed_tables}")
    return successful_tables, failed_tables


def upload_df_as_parquet(target_schema: str, table_name: str, df: pd.DataFrame, logger):
    alp_system_id = os.getenv("ALP__SYSTEM_ID")
    if not alp_system_id:
        raise KeyError("ENV:ALP__SYSTEM_ID is empty")

    bucket_name = f"parquetsnapshots-{alp_system_id}"
    file_name = f"{target_schema}-{table_name}-{int(time()*1000)}.parquet"

    miniodao_module = importlib.import_module("dao.MinioDao")
    minio_dao = miniodao_module.MinioDao()
    try:
        minio_dao.put_dataframe_as_parquet(bucket_name, file_name, df)
    except Exception as err:
        get_run_logger().error(
            f"""Datamart parquet uploading to object store failed at {bucket_name}/{file_name}""")
        raise err
    else:
        get_run_logger().info(f"""Succesfully uploaded parquet file at {
            bucket_name}/{file_name}""")

                
@task(log_prints=True)
def cleanup_target_schema(target_schema_dao):
    target_schema_dao.drop_schema()


@task(log_prints=True)
def create_and_assign_roles(userdao):
    logger = get_run_logger()
    # Check if schema read role exists

    schema_read_role = f"{userdao.schema_name}_read_role"

    schema_read_role_exists = userdao.check_role_exists(schema_read_role)
    if schema_read_role_exists:
        logger.info(f"'{schema_read_role}' role already exists")
    else:
        logger.info(f"{schema_read_role} does not exist")
        userdao.create_read_role(schema_read_role)
    # grant schema read role read privileges to schema
    logger.info(f"Granting read privileges to '{schema_read_role}'")
    userdao.grant_read_privileges(schema_read_role)

    # Check if read user exists
    read_user = userdao.tenant_configs.get("readUser")

    read_user_exists = userdao.check_user_exists(read_user)
    if read_user_exists:
        logger.info(f"{read_user} user already exists")
    else:
        logger.info(f"{read_user} user does not exist")
        read_password = userdao.tenant_configs.get("readPassword")
        logger.info(f"Creating user '{read_user}'")
        userdao.create_user(read_user, read_password)

    # Check if read role exists
    read_role = userdao.tenant_configs.get("readRole")

    read_role_exists = userdao.check_role_exists(read_role)
    if read_role_exists:
        logger.info(f"'{read_role}' role already exists")
    else:
        logger.info(f"'{read_role}' role does not exist")
        logger.info(
            f"'Creating '{read_role}' role and assigning to '{read_user}' user")
        userdao.create_and_assign_role(read_user, read_role)

    # Grant read role read privileges
    logger.info(f"'Granting read privileges to '{read_role}' role")
    userdao.grant_read_privileges(read_role)
    
    

 
def update_dataset_metadata(options: CreateDatamartOptions):
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
