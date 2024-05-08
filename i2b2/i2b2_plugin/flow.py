from prefect.task_runners import SequentialTaskRunner
from prefect import flow, task, get_run_logger
from prefect_shell import ShellOperation
from i2b2_plugin.types import i2b2PluginType
import importlib
import sys
import os



@task
async def setup_plugin(tag_name):
    logger = get_run_logger()
    repo_dir = "i2b2_plugin/i2b2_data"
    path = os.path.join(os.getcwd(), repo_dir)
    
    os.makedirs(f"{path}", 0o777, True)
    os.chdir(f"{path}")
    
    try:
        await _download_source_code(tag_name)
        await _unzip_source_code(tag_name)
    except Exception as e:
        logger.error(e)
        raise(e)

async def _download_source_code(tag_name):
    await ShellOperation(
        commands=[
            f"wget https://github.com/i2b2/i2b2-data/archive/refs/tags/{tag_name}.tar.gz",
        ]).run()
    
async def _unzip_source_code(tag_name):
    await ShellOperation(
        commands=[         
            f"tar -xzf {tag_name}.tar.gz"
        ]).run()



@task
def overwrite_db_properties(tag_name, database_code, schema_name):
    logger = get_run_logger()
    try:
        new_install_dir = f"i2b2-data-{tag_name[1:]}/edu.harvard.i2b2.data/Release_{tag_name[1:4].replace('.','-')}/NewInstall/Crcdata"
        path = os.path.join(os.getcwd(), new_install_dir)
        os.chdir(f"{path}")
        
        dbutils_module = importlib.import_module('alpconnection.dbutils')
        conn_details = dbutils_module.extract_db_credentials(database_code)
        
        database_name = conn_details["databaseName"]
        pg_password = conn_details["adminPassword"]
        
        with open('db.properties', 'w') as file:
            file.write(f'''
                    db.type=postgresql
                    db.username=postgres_tenant_admin_user
                    db.password={pg_password}
                    db.driver=org.postgresql.Driver
                    db.url=jdbc:postgresql://alp-minerva-postgres-1:5432/{database_name}?currentSchema={schema_name}
                    db.project=demo
                       ''')
    
    except Exception as e:
        logger.error(e)
        raise(e)


@task
def create_i2b2_schema(database_code, schema_name):
    sys.path.append('/app/pysrc')
    dbdao_module = importlib.import_module('dao.DBDao')
    types_modules = importlib.import_module('utils.types')
    i2b2_schema = dbdao_module.DBDao(database_code, schema_name, types_modules.PG_TENANT_USERS.ADMIN_USER)
    schema_exists = i2b2_schema.check_schema_exists()
    if schema_exists == False:
        i2b2_schema.create_schema()
    else:
        get_run_logger().info(f"Schema {schema_name} already exists in database {database_code}")
    

@task
def create_crc_tables(version):
    ShellOperation(
        commands=[
            "cat db.properties",
            f"ant -f data_build.xml create_crcdata_tables_release_{version}"
        ]).run()


@task
def create_crc_stored_procedures(version):
    ShellOperation(
        commands=[
            f"ant -f data_build.xml create_procedures_release_{version}"
        ]).run()


@task
def load_demo_data():
    ShellOperation(
        commands = [
            "ant -f data_build.xml db_demodata_load_data"
        ]
    ).run()
    


@flow(log_prints=True, task_runner=SequentialTaskRunner)
def i2b2_plugin(options: i2b2PluginType):
    logger = get_run_logger()
    database_code = options.database_code
    schema_name = options.schema_name
    tag_name = options.tag_name
    try:
        setup_plugin(tag_name)
        create_i2b2_schema(database_code, schema_name)
        overwrite_db_properties(tag_name, database_code, schema_name)
        version = tag_name[1:4].replace(".", "-")
        create_crc_tables(version)
        create_crc_stored_procedures(version)
        #load_demo_data()
    except Exception as e:
        logger.error(e)
        raise(e)

