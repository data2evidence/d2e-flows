from prefect.task_runners import SequentialTaskRunner
from prefect import flow, task, get_run_logger
from prefect_shell import ShellOperation
from i2b2_plugin.types import i2b2PluginType, FlowActionType
from typing import Dict
import importlib
import sys
import os
from datetime import datetime



@flow(log_prints=True, task_runner=SequentialTaskRunner)
def i2b2_plugin(options: i2b2PluginType):
    match options.flow_action_type:
        case FlowActionType.CREATE_DATA_MODEL:
            create_i2b2_datamodel(options)
        case FlowActionType.GET_VERSION_INFO:
            update_dataset_metadata(options)
        case FlowActionType.LOAD_DATA:
            load_demo_data(options)


def update_dataset_metadata(options):
    logger = get_run_logger()
    dataset_list = options.dataset
    token = options.token
    if (dataset_list is None) or (len(dataset_list) == 0):
        logger.debug("No datasets fetched from portal")
    else:
        logger.info(f"Successfully fetched {len(dataset_list)} datasets from portal")
        for dataset in dataset_list:
            get_and_update_attributes(token, dataset)


@task(log_prints=True)
async def setup_plugin(tag_name: str):
    logger = get_run_logger()
    repo_dir = "i2b2_plugin/i2b2_data"
    path = os.path.join(os.getcwd(), repo_dir)
    
    os.makedirs(f"{path}", 0o777, True)
    os.chdir(f"{path}")
    
    try:
        await _download_source_code(tag_name)
        await _unzip_source_code(tag_name)
        #await _setup_apache_ant(tag_name) # use version of apache ant in i2b2 source code
    except Exception as e:
        logger.error(e)
        raise(e)

async def _download_source_code(tag_name: str):
    await ShellOperation(
        commands=[
            f"wget https://github.com/i2b2/i2b2-data/archive/refs/tags/{tag_name}.tar.gz",
        ]).run()
    
async def _unzip_source_code(tag_name: str):
    await ShellOperation(
        commands=[         
            f"tar -xzf {tag_name}.tar.gz"
        ]).run()

async def _setup_apache_ant(tag_name: str):
    cwd = os.getcwd()
    ant_bin_dir = os.path.join(cwd, f"{_path_to_ant(tag_name)}/apache-ant")
    
    # Set ant_home environment variable
    os.environ["ANT_HOME"] = ant_bin_dir
    
    await ShellOperation(
        commands=[         
            f'ln -sfn {ant_bin_dir} /opt/ant',
            f'ln -sfn /opt/ant/bin/ant /usr/bin/ant',
            'ant -version'
        ]).run()


@task(log_prints=True)
def overwrite_db_properties(tag_name: str, tenant_configs: Dict, schema_name: str):
    logger = get_run_logger()
    try:
        new_install_dir = f"{_path_to_ant(tag_name)}/NewInstall/Crcdata"
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
def update_creation_date():
    pass
    

@task
def create_crc_tables(version: str):
    ShellOperation(
        commands=[
            f"ant -f data_build.xml create_crcdata_tables_release_{version}"
        ]).run()


@task
def create_crc_stored_procedures(version: str):
    ShellOperation(
        commands=[
            f"ant -f data_build.xml create_procedures_release_{version}"
        ]).run()


@flow
def load_demo_data():
    ingest_data()
    update_ingestion_date()


@task
def ingest_data():
    ShellOperation(
        commands = [
            "ant -f data_build.xml db_demodata_load_data"
        ]
    ).run()
    
@task
def update_ingestion_date():
    pass
    

def create_i2b2_datamodel(options: i2b2PluginType):
    logger = get_run_logger()
    database_code = options.database_code
    schema_name = options.schema_name
    tag_name = options.tag_name
    token = options.token
    try:
        sys.path.append('/app/pysrc')
        dbdao_module = importlib.import_module('dao.DBDao')
        types_modules = importlib.import_module('utils.types')
        userdao_module = importlib.import_module('dao.UserDao')
        dbsvc_module = importlib.import_module('flows.alp_db_svc.dataset.main')
        dbutils_module = importlib.import_module('alpconnection.dbutils')
        portal_server_api_module = importlib.import_module('api.PortalServerAPI')
        
        portal_server_api = portal_server_api_module.PortalServerAPI(token)
        
        admin_user = types_modules.PG_TENANT_USERS.ADMIN_USER
        dbdao = dbdao_module.DBDao(database_code, schema_name, admin_user)
        userdao = userdao_module.UserDao(database_code, schema_name, admin_user)
        tenant_configs = dbutils_module.extract_db_credentials(database_code)
        
        
        setup_plugin(tag_name)
        create_i2b2_schema(dbdao)
        overwrite_db_properties(tag_name, tenant_configs, schema_name)
        version = _get_version(tag_name)
        create_crc_tables(version)
        create_crc_stored_procedures(version)
        
        dbdao.create_i2b2_metadata_table()
        
        # grant read privilege to tenant read user
        dbsvc_module.create_and_assign_roles(
            userdao=userdao,
            tenant_configs=tenant_configs,
            data_model="i2b2",
            dialect=types_modules.DatabaseDialects.POSTGRES
        )

        # load demo data
        if options.load_data:
            load_demo_data() # subflow

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




        


@task
def get_and_update_attributes(token: str, dataset: Dict):
    sys.path.append('/app/pysrc')
    dbdao_module = importlib.import_module('dao.DBDao')
    types_modules = importlib.import_module('utils.types')
    portal_server_api_module = importlib.import_module('api.PortalServerAPI')
    
    admin_user = types_modules.PG_TENANT_USERS.ADMIN_USER
        
    try:
        database_code = dataset.get("databaseCode")
        schema_name = dataset.get("schemaName")
        dataset_id = dataset.get("id")
    except KeyError:
        get_run_logger().error()     
    else:
        try:
            # update with patient count or error msg
            dbdao = dbdao_module.DBDao(database_code, schema_name, admin_user)
            patient_count = dbdao.get_distinct_count("patient_dimension", "patient_num")
        except Exception as e:
            error_msg = f"Error retrieving patient count"
            get_run_logger().error(f"{error_msg}: {e}")
            patient_count = error_msg
            
        portal_server_api = portal_server_api_module.PortalServerAPI(token)
        portal_server_api.update_dataset_attributes_table(dataset_id, "patient_count", patient_count)

        

        

def _get_version(tag: str) -> str:
    return tag[1:4].replace(".", "-")

def _path_to_ant(tag: str) -> str:
    return f"i2b2-data-{tag[1:]}/edu.harvard.i2b2.data/Release_{_get_version(tag)}"

