import os
from functools import partial
from datetime import datetime
from sqlalchemy import String, TIMESTAMP

from prefect_shell import ShellOperation
from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from flows.i2b2_plugin.types import *
from flows.i2b2_plugin.utils import *

from shared_utils.dao.DBDao import DBDao
from shared_utils.dao.UserDao import UserDao
from shared_utils.update_dataset_metadata import *
from shared_utils.api.PortalServerAPI import PortalServerAPI
from shared_utils.create_dataset_tasks import create_and_assign_roles_task, drop_schema_hook



@flow(log_prints=True, task_runner=SequentialTaskRunner, timeout_seconds=3600)
def i2b2_plugin(options: i2b2PluginType):
    match options.flow_action_type:
        case FlowActionType.CREATE_DATA_MODEL:
            create_i2b2_dataset(options)
        case FlowActionType.GET_VERSION_INFO:
            update_dataset_metadata(options)


def create_i2b2_dataset(options: i2b2PluginType):
    logger = get_run_logger()

    database_code = options.database_code
    schema_name = options.schema_name
    tag_name = options.tag_name
    data_model = options.data_model
    use_cache_db = options.use_cache_db

    try:
        dbdao = DBDao(use_cache_db=use_cache_db,
                      database_code=database_code, 
                      schema_name=schema_name)
        
        userdao = UserDao(use_cache_db=use_cache_db,
                          database_code=database_code, 
                          schema_name=schema_name)
        
        tenant_configs = dbdao.tenant_configs
        
        
        setup_plugin(tag_name)
        create_i2b2_schema(dbdao)
        overwrite_db_properties(tag_name, tenant_configs, schema_name)

        version = get_version_from_tag(tag_name)

        create_crc_tables(version)
        create_crc_stored_procedures(version)
        
        # task to create i2b2 metadata table
        create_metadata_table(dbdao, tag_name, data_model[1:])
        
        # prefect task to grant read privilege to tenant read user
        create_and_assign_roles_wo = create_and_assign_roles_task.with_options(
            on_failure=[partial(drop_schema_hook,
                                **dict(schema_dao=dbdao))]
        )        
        
        create_and_assign_roles_wo(
            userdao=userdao
        )
        
        # task to load demo data based on flag
        if options.load_demo_data:
            load_demo_data(dbdao)

    except Exception as e:
        logger.error(e)
        raise(e)


def update_dataset_metadata(options: i2b2PluginType):
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
async def setup_plugin(tag_name: str):
    logger = get_run_logger()
    repo_dir = "flows/i2b2_plugin/i2b2_data"
    path = os.path.join(os.getcwd(), repo_dir)
    
    os.makedirs(f"{path}", 0o777, True)
    os.chdir(f"{path}")
    
    try:
        await download_source_code(tag_name)
        await unzip_source_code(tag_name)
        #await setup_apache_ant(tag_name) # use version of apache ant in i2b2 source code
    except Exception as e:
        logger.error(e)
        raise(e)


@task(log_prints=True)
def overwrite_db_properties(tag_name: str, tenant_configs: dict, schema_name: str):
    logger = get_run_logger()
    try:
        new_install_dir = f"{path_to_ant(tag_name)}/NewInstall/Crcdata"
        path = os.path.join(os.getcwd(), new_install_dir)
        os.chdir(f"{path}")
        
        database_name = tenant_configs["databaseName"]
        pg_user = tenant_configs["adminUser"]
        pg_password = tenant_configs["adminPassword"]
        host = tenant_configs["host"]
        port = tenant_configs["port"]
        
        with open('db.properties', 'w') as file:
            file.write(f'''
                    db.type=postgresql
                    db.username={pg_user}
                    db.password={pg_password}
                    db.driver=org.postgresql.Driver
                    db.url=jdbc:postgresql://{host}:{port}/{database_name}?currentSchema={schema_name}
                    db.project=demo
                       ''')
    
    except Exception as e:
        logger.error(e)
        raise(e)


@task(log_prints=True)
def create_i2b2_schema(dbdao):
    schema_exists = dbdao.check_schema_exists()
    if schema_exists == False:
        dbdao.create_schema()
    else:
        raise Exception(f"Schema {dbdao.schema_name} already exists in database {dbdao.database_code}")

@task(log_prints=True)
def create_crc_tables(version: str):
    ShellOperation(
        commands=[
            f"ant -f data_build.xml create_crcdata_tables_release_{version}"
        ]).run()


@task(log_prints=True)
def create_crc_stored_procedures(version: str):
    ShellOperation(
        commands=[
            f"ant -f data_build.xml create_procedures_release_{version}"
        ]).run()


@task(log_prints=True)
def load_demo_data(dbdao):
    ingest_data()
    dbdao.update_data_ingestion_date()


@task(log_prints=True)
def create_metadata_table(dbdao, tag_name: str, version: str):
    columns_to_create = {
            "schema_name": String,
            "created_date": TIMESTAMP,
            "updated_date": TIMESTAMP,
            "data_ingestion_date": TIMESTAMP,
            "tag": String,
            "release_version": String
        }
    dbdao.create_table('dataset_metadata', columns_to_create)
    values_to_insert = {
        "schema_name": dbdao.schema_name,
        "created_date": datetime.now(),
        "updated_date": datetime.now(),
        "tag": tag_name,
        "release_version": version
    }
    dbdao.insert_values_into_table('dataset_metadata', values_to_insert)
        

def ingest_data():
    ShellOperation(
        commands = [
            "ant -f data_build.xml db_demodata_load_data"
        ]
    ).run()
    


@task(log_prints=True)
def get_and_update_attributes(token: str, dataset: dict, use_cache_db: bool):
    logger = get_run_logger()
    
    try:
        dataset_id = dataset.get("id")
        database_code = dataset.get("databaseCode")
        schema_name = dataset.get("schemaName")
        data_model = dataset.get("dataModel").split(" ")[0]
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
        if schema_exists == False:
            error_msg = f"Schema '{schema_name}' does not exist in db {database_code} for dataset id '{dataset_id}'"
            logger.error(error_msg)
            portal_server_api.update_dataset_attributes_table(dataset_id, "schema_version", error_msg)
            portal_server_api.update_dataset_attributes_table(dataset_id, "latest_schema_version", error_msg)
        else:
            # update patient count or error msg
            update_entity_distinct_count(
                portal_server_api=portal_server_api,
                dataset_id=dataset_id,
                dbdao=dbdao,
                table_name="patient_dimension",
                column_name="patient_num",
                entity_name="patient_count",
                logger=logger
                )                
                
            try:
                # update release version or error msg
                release_version = data_model[1:]
                portal_server_api.update_dataset_attributes_table(dataset_id, "version", release_version)
            except Exception as e:
                logger.error(f"Failed to update attribute 'version' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'version' for dataset '{dataset_id}' with value '{release_version}'")

            try:
                # update release tag or error msg
                tag = RELEASE_TAG_MAPPING.get(data_model)
                portal_server_api.update_dataset_attributes_table(dataset_id, "schema_version", tag)
                portal_server_api.update_dataset_attributes_table(dataset_id, "latest_schema_version", tag)
            except Exception as e:
                logger.error(f"Failed to update attribute 'schema_version', 'latest_schema_version' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'schema_version', 'latest_schema_version' for dataset '{dataset_id}' with value '{tag}'")
                
            try:
                # update created date or error msg
                created_date = get_metadata_date(dbdao, "created_date")
                portal_server_api.update_dataset_attributes_table(dataset_id, "created_date", created_date)
            except Exception as e:
                logger.error(f"Failed to update attribute 'created_date' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'created_date' for dataset '{dataset_id}' with value '{created_date}'")
                
            try:
                # update updated date or error msg
                updated_date = get_metadata_date(dbdao, "updated_date")
                portal_server_api.update_dataset_attributes_table(dataset_id, "updated_date", updated_date)
            except Exception as e:
                logger.error(f"Failed to update attribute 'updated_date' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'updated_date' for dataset '{dataset_id}' with value '{updated_date}'")
                            
            try:
                # update data ingestion date or error msg
                data_ingestion_date = get_metadata_date(dbdao, "data_ingestion_date")
                portal_server_api.update_dataset_attributes_table(dataset_id, "data_ingestion_date", data_ingestion_date)
            except Exception as e:
                logger.error(f"Failed to update attribute 'data_ingestion_date' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'data_ingestion_date' for dataset '{dataset_id}' with value '{data_ingestion_date}'")

            try:
                # update last fetched metadata date
                metadata_last_fetch_date = datetime.now().strftime('%Y-%m-%d')
                portal_server_api.update_dataset_attributes_table(dataset_id, "metadata_last_fetch_date", metadata_last_fetch_date)
            except Exception as e:
                logger.error(f"Failed to update attribute 'metadata_last_fetch_date' for dataset '{dataset_id}': {e}")
            else:
                logger.info(f"Updated attribute 'metadata_last_fetch_date' for dataset '{dataset_id}' with value '{metadata_last_fetch_date}'")

    
            # update last fetched metadata date
            update_metadata_last_fetched_date(
                portal_server_api=portal_server_api,
                dataset_id=dataset_id,
                logger=logger
            )